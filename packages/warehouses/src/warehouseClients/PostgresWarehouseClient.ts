import {
    AnyType,
    CreatePostgresCredentials,
    CreatePostgresLikeCredentials,
    DimensionType,
    getErrorMessage,
    Metric,
    MetricType,
    setCatalogTimestampDomain,
    SslConfiguration,
    SupportedDbtAdapter,
    WarehouseCatalog,
    WarehouseQueryError,
    WarehouseResults,
    WarehouseTypes,
    type TimestampDomain,
    type WarehouseQueryPhase,
} from '@lightdash/common';
import { readFileSync } from 'fs';
import path from 'path';
import * as pg from 'pg';
import { PoolConfig, QueryResult, types } from 'pg';
import { Writable } from 'stream';
import * as tls from 'tls';
import { rootCertificates } from 'tls';
import { normalizeUnicode } from '../utils/sql';
import './pgProtocolGuard';
import QueryStream from './PgQueryStream';
import WarehouseBaseClient from './WarehouseBaseClient';
import WarehouseBaseSqlBuilder from './WarehouseBaseSqlBuilder';

types.setTypeParser(types.builtins.NUMERIC, (value) => parseFloat(value));
types.setTypeParser(types.builtins.INT8, BigInt);

export enum PostgresTypes {
    INTEGER = 'integer',
    INT = 'int',
    INT2 = 'int2',
    INT4 = 'int4',
    INT8 = 'int8',
    MONEY = 'money',
    SMALLSERIAL = 'smallserial',
    SERIAL = 'serial',
    SERIAL2 = 'serial2',
    SERIAL4 = 'serial4',
    SERIAL8 = 'serial8',
    BIGSERIAL = 'bigserial',
    BIGINT = 'bigint',
    SMALLINT = 'smallint',
    BOOLEAN = 'boolean',
    BOOL = 'bool',
    DATE = 'date',
    DOUBLE_PRECISION = 'double precision',
    FLOAT = 'float',
    FLOAT4 = 'float4',
    FLOAT8 = 'float8',
    JSON = 'json',
    JSONB = 'jsonb',
    NUMERIC = 'numeric',
    DECIMAL = 'decimal',
    REAL = 'real',
    CHAR = 'char',
    CHARACTER = 'character',
    NCHAR = 'nchar',
    BPCHAR = 'bpchar',
    VARCHAR = 'varchar',
    CHARACTER_VARYING = 'character varying',
    NVARCHAR = 'nvarchar',
    TEXT = 'text',
    TIME = 'time',
    TIME_TZ = 'timetz',
    TIME_WITHOUT_TIME_ZONE = 'time without time zone',
    TIMESTAMP = 'timestamp',
    TIMESTAMP_TZ = 'timestamptz',
    TIMESTAMP_WITHOUT_TIME_ZONE = 'timestamp without time zone',
    TIMESTAMP_WITH_TIME_ZONE = 'timestamp with time zone',
}

const mapFieldType = (type: string): DimensionType => {
    switch (type) {
        case PostgresTypes.DECIMAL:
        case PostgresTypes.NUMERIC:
        case PostgresTypes.INTEGER:
        case PostgresTypes.MONEY:
        case PostgresTypes.SMALLSERIAL:
        case PostgresTypes.SERIAL:
        case PostgresTypes.SERIAL2:
        case PostgresTypes.SERIAL4:
        case PostgresTypes.SERIAL8:
        case PostgresTypes.BIGSERIAL:
        case PostgresTypes.INT2:
        case PostgresTypes.INT4:
        case PostgresTypes.INT8:
        case PostgresTypes.BIGINT:
        case PostgresTypes.SMALLINT:
        case PostgresTypes.FLOAT:
        case PostgresTypes.FLOAT4:
        case PostgresTypes.FLOAT8:
        case PostgresTypes.DOUBLE_PRECISION:
        case PostgresTypes.REAL:
            return DimensionType.NUMBER;
        case PostgresTypes.DATE:
            return DimensionType.DATE;
        case PostgresTypes.TIME:
        case PostgresTypes.TIME_TZ:
        case PostgresTypes.TIMESTAMP:
        case PostgresTypes.TIMESTAMP_TZ:
        case PostgresTypes.TIME_WITHOUT_TIME_ZONE:
        case PostgresTypes.TIMESTAMP_WITHOUT_TIME_ZONE:
        case PostgresTypes.TIMESTAMP_WITH_TIME_ZONE:
            return DimensionType.TIMESTAMP;
        case PostgresTypes.BOOLEAN:
        case PostgresTypes.BOOL:
            return DimensionType.BOOLEAN;
        default:
            return DimensionType.STRING;
    }
};

// Strips the precision that format_type emits, e.g. `timestamp(3) with time zone`
export const getPostgresTimestampDomain = (
    type: string,
): TimestampDomain | undefined => {
    switch (type.replace(/\(\d+\)/, '').replace(/\s{2,}/g, ' ')) {
        case PostgresTypes.TIMESTAMP:
        case PostgresTypes.TIMESTAMP_WITHOUT_TIME_ZONE:
            return 'naive';
        case PostgresTypes.TIMESTAMP_TZ:
        case PostgresTypes.TIMESTAMP_WITH_TIME_ZONE:
            return 'aware';
        default:
            return undefined;
    }
};

const { builtins } = pg.types;
const POSTGRES_NAME_TOO_LONG_SQLSTATE = '42622';

// Server-side ceiling for a single streamed query, bounded just under the
// 10-min scheduler job timeout so a stalled cursor fails clearly instead of
// hanging the whole job. The pool's `query_timeout` does not fire on the
// cursor (pg-cursor) path, so the ceiling is enforced via `statement_timeout`
// plus a client-side wall-clock backstop. Overridable per-connection via
// `timeoutSeconds`.
const DEFAULT_STATEMENT_TIMEOUT_MS = 1000 * 60 * 9; // 9 minutes

// The client-side backstop fires this long after the server-side
// statement_timeout, so the server's cancellation error wins the race and
// surfaces a clear message; the client backstop only triggers if the server
// never reports back (e.g. a dead SSH tunnel socket).
const CLIENT_STATEMENT_TIMEOUT_BUFFER_MS = 1000 * 30; // 30 seconds

const CLEANUP_ROLLBACK_TIMEOUT_MS = 1500;
const CLEANUP_POOL_END_TIMEOUT_MS = 2000;

const withTimeout = async <T>(
    promise: Promise<T>,
    timeoutMs: number,
    label: string,
): Promise<T> => {
    let timer: ReturnType<typeof setTimeout> | undefined;
    try {
        return await Promise.race([
            promise,
            new Promise<T>((_, reject) => {
                timer = setTimeout(() => {
                    reject(
                        new Error(`${label} timed out after ${timeoutMs}ms`),
                    );
                }, timeoutMs);
            }),
        ]);
    } finally {
        if (timer) {
            clearTimeout(timer);
        }
    }
};

const forceDestroyClient = (client: pg.PoolClient | undefined): void => {
    if (!client) {
        return;
    }
    const maybeClient = client as pg.PoolClient & {
        connection?: { stream?: { destroy: (err?: Error) => void } };
        end?: () => Promise<void>;
    };
    if (maybeClient.connection?.stream?.destroy) {
        maybeClient.connection.stream.destroy(
            new Error('postgres warehouse client force-destroyed'),
        );
        return;
    }
    if (maybeClient.end) {
        void maybeClient.end().catch(() => undefined);
    }
};

const convertDataTypeIdToDimensionType = (
    dataTypeId: number,
): DimensionType => {
    switch (dataTypeId) {
        case builtins.NUMERIC:
        case builtins.MONEY:
        case builtins.INT2:
        case builtins.INT4:
        case builtins.INT8:
        case builtins.FLOAT4:
        case builtins.FLOAT8:
            return DimensionType.NUMBER;
        case builtins.DATE:
            return DimensionType.DATE;
        case builtins.TIME:
        case builtins.TIMETZ:
        case builtins.TIMESTAMP:
        case builtins.TIMESTAMPTZ:
            return DimensionType.TIMESTAMP;
        case builtins.BOOL:
            return DimensionType.BOOLEAN;
        default:
            return DimensionType.STRING;
    }
};

export class PostgresSqlBuilder extends WarehouseBaseSqlBuilder {
    type = WarehouseTypes.POSTGRES;

    getAdapterType(): SupportedDbtAdapter {
        return SupportedDbtAdapter.POSTGRES;
    }

    getEscapeStringQuoteChar(): string {
        return "'";
    }

    escapeString(value: string): string {
        if (typeof value !== 'string') {
            return value;
        }

        return (
            normalizeUnicode(value)
                // Escape single quotes by doubling them (PostgreSQL standard)
                .replaceAll("'", "''")
                // PostgreSQL LIKE wildcards need to be escaped with backslashes
                .replaceAll('\\', '\\\\') // Escape backslashes first
                // Remove SQL comments (-- and /* */)
                .replace(/--.*$/gm, '')
                .replace(/\/\*[\s\S]*?\*\//g, '')
                // Remove null bytes
                .replaceAll('\0', '')
        );
    }

    getMetricSql(sql: string, metric: Metric): string {
        switch (metric.type) {
            case MetricType.AVERAGE:
                return `AVG(${sql}::DOUBLE PRECISION)`;
            case MetricType.PERCENTILE:
                return `PERCENTILE_CONT(${
                    (metric.percentile ?? 50) / 100
                }) WITHIN GROUP (ORDER BY ${sql})`;
            case MetricType.MEDIAN:
                return `PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY ${sql})`;
            default:
                return super.getMetricSql(sql, metric);
        }
    }

    concatString(...args: string[]): string {
        return `(${args.join(' || ')})`;
    }
}

export class PostgresClient<
    T extends CreatePostgresLikeCredentials,
> extends WarehouseBaseClient<T> {
    config: pg.PoolConfig;

    constructor(credentials: T, config: pg.PoolConfig) {
        super(credentials, new PostgresSqlBuilder(credentials.startOfWeek));
        this.config = config;
    }

    private getSQLWithMetadata(sql: string, tags?: Record<string, string>) {
        let alteredQuery = sql;
        if (tags) {
            alteredQuery = `${alteredQuery}\n-- ${JSON.stringify(tags)}`;
        }
        return alteredQuery;
    }

    static convertQueryResultFields(
        fields: QueryResult<AnyType>['fields'],
    ): WarehouseResults['fields'] {
        return Object.fromEntries(
            fields.map(({ name, dataTypeID }) => [
                name,
                { type: convertDataTypeIdToDimensionType(dataTypeID) },
            ]),
        );
    }

    private static getNoticeError(notice: {
        code?: string;
        message?: string;
    }): WarehouseQueryError | undefined {
        if (notice.code !== POSTGRES_NAME_TOO_LONG_SQLSTATE) {
            return undefined;
        }

        return new WarehouseQueryError(
            `PostgreSQL identifier is too long: ${notice.message}`,
        );
    }

    async streamQuery(
        sql: string,
        streamCallback: (data: WarehouseResults) => void | Promise<void>,
        options: {
            values?: AnyType[];
            tags?: Record<string, string>;
            timezone?: string;
            onPhaseTiming?: (
                phase: WarehouseQueryPhase,
                durationMs: number,
            ) => void;
        },
    ): Promise<void> {
        let pool: pg.Pool | undefined;
        let closeClient: ((err?: Error) => void) | undefined;
        let poolClient: pg.PoolClient | undefined;
        let transactionOpen = false;
        let cleanSuccess = false;
        let activeStream: QueryStream | undefined;
        let queryTimeout: ReturnType<typeof setTimeout> | undefined;

        const reportPhase = options.onPhaseTiming;

        // The pool's `query_timeout` does not fire on the cursor (pg-cursor)
        // path, so we enforce the ceiling ourselves: a server-side
        // `statement_timeout` (set below) plus this client-side wall-clock
        // backstop that fires shortly after, in case the server never reports
        // back (e.g. a stalled SSH tunnel socket).
        const statementTimeoutMs = this.credentials.timeoutSeconds
            ? this.credentials.timeoutSeconds * 1000
            : DEFAULT_STATEMENT_TIMEOUT_MS;
        const clientTimeoutMs =
            statementTimeoutMs + CLIENT_STATEMENT_TIMEOUT_BUFFER_MS;

        return new Promise<void>((resolve, reject) => {
            queryTimeout = setTimeout(() => {
                const timeoutError = new WarehouseQueryError(
                    `Query timed out after ${Math.round(
                        clientTimeoutMs / 1000,
                    )}s`,
                );
                activeStream?.destroy(timeoutError);
                reject(timeoutError);
            }, clientTimeoutMs);

            pool = new pg.Pool({
                ...this.config,
                connectionTimeoutMillis: 30000,
                query_timeout: this.credentials.timeoutSeconds
                    ? this.credentials.timeoutSeconds * 1000
                    : 1000 * 60 * 5, // sets the default query timeout to 5 minutes
            });

            pool.on('error', (err) => {
                console.error(`Postgres pool error ${getErrorMessage(err)}`);
                reject(err);
            });

            pool.on('connect', (_client: pg.PoolClient) => {
                // On each new client initiated, need to register for error(this is a serious bug on pg, the client throw errors although it should not)
                _client.on('error', (err: Error) => {
                    console.error(
                        `Postgres client connect error ${getErrorMessage(err)}`,
                    );
                    reject(err);
                });
            });

            const connectStart = performance.now();
            pool.connect((err, client, done) => {
                // Store references so we can clean up properly
                closeClient = done;

                if (err) {
                    reject(err);
                    return;
                }
                if (!client) {
                    reject(new Error('client undefined'));
                    return;
                }
                poolClient = client;
                reportPhase?.('connect', performance.now() - connectStart);

                client.on('error', (e) => {
                    console.error(
                        `Postgres client error ${getErrorMessage(e)}`,
                    );
                    reject(e);
                });

                client.on('notice', (notice) => {
                    const error = PostgresClient.getNoticeError(notice);
                    if (!error) {
                        return;
                    }

                    activeStream?.destroy(error);
                    reject(error);
                });

                const runQuery = () => {
                    const queryStart = performance.now();
                    let fetchStart: number | undefined;
                    // CodeQL: This will raise a security warning because user defined raw SQL is being passed into the database module.
                    //         In this case this is exactly what we want to do. We're hitting the user's warehouse not the application's database.
                    activeStream = client.query(
                        // callback is not defined in types when using QueryStream
                        // @ts-ignore
                        new QueryStream(
                            this.getSQLWithMetadata(sql, options?.tags),
                            options?.values,
                        ),
                        // there is a bug in PG lib where callback is required when passing `query_timeout` to the Pool
                        // see the code: https://github.com/brianc/node-postgres/blob/master/packages/pg/lib/client.js#L541-L542
                        () => {},
                        // typecast is necessary to fix the type issue described above
                    ) as unknown as QueryStream;

                    // Cache field conversion — result.fields is the same
                    // array reference for every row in a query, so we only
                    // need to convert it once.
                    let cachedFields: Record<
                        string,
                        { type: DimensionType }
                    > | null = null;

                    const writable = new Writable({
                        objectMode: true,
                        async write(
                            chunk: {
                                row: AnyType;
                                fields: QueryResult<AnyType>['fields'];
                            },
                            encoding,
                            callback,
                        ) {
                            try {
                                if (cachedFields === null) {
                                    cachedFields =
                                        PostgresClient.convertQueryResultFields(
                                            chunk.fields,
                                        );
                                    reportPhase?.(
                                        'query',
                                        performance.now() - queryStart,
                                    );
                                    fetchStart = performance.now();
                                }
                                await streamCallback({
                                    fields: cachedFields,
                                    rows: [chunk.row],
                                });
                                callback();
                            } catch (writeError) {
                                if (writeError instanceof Error) {
                                    callback(writeError);
                                } else {
                                    callback(new Error(String(writeError)));
                                }
                            }
                        },
                    });

                    // Wait for writable to finish processing all async callbacks
                    // (not 'end' on readable - async write callbacks may still be in flight).
                    // COMMIT keeps the pg-cursor portal on one backend under
                    // transaction-mode poolers (pgdog/pgbouncer).
                    writable.on('finish', () => {
                        if (fetchStart === undefined) {
                            reportPhase?.(
                                'query',
                                performance.now() - queryStart,
                            );
                            reportPhase?.('fetch', 0);
                        } else {
                            reportPhase?.(
                                'fetch',
                                performance.now() - fetchStart,
                            );
                        }
                        client
                            .query('COMMIT')
                            .then(() => {
                                transactionOpen = false;
                                cleanSuccess = true;
                                resolve();
                            })
                            .catch((commitError) => {
                                reject(commitError);
                            });
                    });
                    writable.on('error', (err2) => {
                        activeStream?.destroy(err2);
                        reject(err2);
                    });
                    activeStream.on('error', (err2) => {
                        reject(err2);
                    });
                    activeStream.pipe(writable).on('error', (err2) => {
                        activeStream?.destroy(err2);
                        reject(err2);
                    });
                };

                // Open an explicit transaction before session setup and the
                // cursor stream. pg-cursor uses a named portal across batches;
                // transaction-mode poolers drop portals between implicit
                // transactions, so BEGIN...COMMIT pins one backend for the
                // whole stream. statement_timeout is also set inside the txn
                // so it applies on the same backend as the cursor.
                const sessionStart = performance.now();
                client
                    .query('BEGIN')
                    .then(() => {
                        transactionOpen = true;
                        return client.query(
                            `SET statement_timeout = ${statementTimeoutMs}`,
                        );
                    })
                    .then(() => {
                        if (options?.timezone) {
                            console.debug(
                                `Setting postgres session timezone ${options?.timezone}`,
                            );
                            return client.query(
                                `SET timezone TO '${options?.timezone}'`,
                            );
                        }
                        return undefined;
                    })
                    .then(() => {
                        reportPhase?.(
                            'session',
                            performance.now() - sessionStart,
                        );
                        runQuery();
                    })
                    .catch((sessionError) => {
                        reject(sessionError);
                    });
            });
        })
            .catch((e) => {
                if (e instanceof WarehouseQueryError) {
                    throw e;
                }
                const error = e as pg.DatabaseError;
                throw this.parseError(error, sql);
            })
            .finally(async () => {
                if (queryTimeout) {
                    clearTimeout(queryTimeout);
                }

                let rollbackOutcome:
                    | 'success'
                    | 'error'
                    | 'timeout'
                    | 'skipped' = 'skipped';
                let releaseMode: 'soft' | 'destroy' | 'none' = 'none';
                let poolEndOutcome:
                    | 'success'
                    | 'error'
                    | 'timeout'
                    | 'skipped' = 'skipped';
                let discardClient = !cleanSuccess;

                if (transactionOpen && poolClient) {
                    try {
                        await withTimeout(
                            poolClient.query('ROLLBACK'),
                            CLEANUP_ROLLBACK_TIMEOUT_MS,
                            'postgres ROLLBACK',
                        );
                        rollbackOutcome = 'success';
                    } catch (rollbackError) {
                        discardClient = true;
                        rollbackOutcome =
                            rollbackError instanceof Error &&
                            rollbackError.message.includes('timed out')
                                ? 'timeout'
                                : 'error';
                        console.warn(
                            'Error rolling back postgres transaction:',
                            rollbackError,
                        );
                    }
                    transactionOpen = false;
                }

                if (closeClient) {
                    try {
                        if (discardClient) {
                            releaseMode = 'destroy';
                            closeClient(
                                new Error(
                                    'postgres warehouse stream failed; discarding client',
                                ),
                            );
                        } else {
                            releaseMode = 'soft';
                            closeClient();
                        }
                    } catch (releaseError) {
                        discardClient = true;
                        console.warn('Error releasing client:', releaseError);
                        forceDestroyClient(poolClient);
                    }
                }

                if (pool) {
                    try {
                        await withTimeout(
                            pool.end(),
                            CLEANUP_POOL_END_TIMEOUT_MS,
                            'postgres pool.end',
                        );
                        poolEndOutcome = 'success';
                    } catch (poolError) {
                        poolEndOutcome =
                            poolError instanceof Error &&
                            poolError.message.includes('timed out')
                                ? 'timeout'
                                : 'error';
                        console.info('Failed to end postgres pool:', poolError);
                        forceDestroyClient(poolClient);
                    }
                }

                if (!cleanSuccess) {
                    console.warn(
                        `Postgres warehouse stream cleanup: rollback=${rollbackOutcome} release=${releaseMode} pool.end=${poolEndOutcome}`,
                    );
                }
            });
    }

    async getCatalog(
        requests: {
            database: string;
            schema: string;
            table: string;
        }[],
    ) {
        const { databases, schemas, tables } = requests.reduce<{
            databases: Set<string>;
            schemas: Set<string>;
            tables: Set<string>;
        }>(
            (acc, { database, schema, table }) => ({
                databases: acc.databases.add(`'${database}'`),
                schemas: acc.schemas.add(`'${schema}'`),
                tables: acc.tables.add(`'${table}'`),
            }),
            {
                databases: new Set(),
                schemas: new Set(),
                tables: new Set(),
            },
        );
        if (databases.size <= 0 || schemas.size <= 0 || tables.size <= 0) {
            return {};
        }

        const { rows: pgVersionRows } = await this.runQuery('SELECT version()');
        const pgVersionString = pgVersionRows[0]?.version ?? '';
        const versionRegex = /PostgreSQL (\d+)\./;
        const versionMatch = pgVersionString.match(versionRegex);
        const supportsMatviews =
            versionMatch && versionMatch[1]
                ? parseInt(versionMatch[1], 10) >= 12
                : false;

        const query = `
            SELECT table_catalog,
                   table_schema,
                   table_name,
                   column_name,
                   data_type
            FROM information_schema.columns
            WHERE table_catalog IN (${Array.from(databases)})
              AND table_schema IN (${Array.from(schemas)})
              AND table_name IN (${Array.from(tables)})
            ${
                supportsMatviews
                    ? `

            UNION ALL

            SELECT current_database() AS table_catalog,
                n.nspname AS table_schema,
                c.relname AS table_name,
                a.attname AS column_name,
                pg_catalog.format_type(a.atttypid, a.atttypmod) AS data_type
            FROM pg_catalog.pg_attribute a
            JOIN pg_catalog.pg_class c ON a.attrelid = c.oid
            JOIN pg_catalog.pg_namespace n ON c.relnamespace = n.oid
            JOIN pg_catalog.pg_matviews mv ON n.nspname = mv.schemaname AND c.relname = mv.matviewname
            WHERE c.relkind = 'm'
            AND current_database() IN (${Array.from(databases)})
            AND n.nspname IN (${Array.from(schemas)})
            AND c.relname IN (${Array.from(tables)})
            AND a.attnum > 0
            AND NOT a.attisdropped`
                    : ''
            }`;

        const { rows } = await this.runQuery(query);
        const catalog = rows.reduce(
            (
                acc,
                {
                    table_catalog,
                    table_schema,
                    table_name,
                    column_name,
                    data_type,
                },
            ) => {
                const match = requests.find(
                    ({ database, schema, table }) =>
                        database === table_catalog &&
                        schema === table_schema &&
                        table === table_name,
                );
                if (match) {
                    acc[table_catalog] = acc[table_catalog] || {};
                    acc[table_catalog][table_schema] =
                        acc[table_catalog][table_schema] || {};
                    acc[table_catalog][table_schema][table_name] =
                        acc[table_catalog][table_schema][table_name] || {};
                    acc[table_catalog][table_schema][table_name][column_name] =
                        mapFieldType(data_type);
                    setCatalogTimestampDomain(
                        acc,
                        table_catalog,
                        table_schema,
                        table_name,
                        column_name,
                        getPostgresTimestampDomain(data_type),
                    );
                }

                return acc;
            },
            {},
        );
        return catalog;
    }

    async getAllTables() {
        const databaseName = this.config.database;
        const whereSql = databaseName ? `AND table_catalog = $1` : '';
        const filterSystemTables = `AND table_schema NOT IN ('information_schema', 'pg_catalog')`;
        const query = `
            SELECT table_catalog, table_schema, table_name
            FROM information_schema.tables
            WHERE table_type = 'BASE TABLE'
                ${whereSql}
                ${filterSystemTables}
            ORDER BY 1, 2, 3
        `;
        const { rows } = await this.runQuery(
            query,
            {},
            undefined,
            databaseName ? [databaseName] : [],
        );
        return rows.map((row) => ({
            database: row.table_catalog,
            schema: row.table_schema,
            table: row.table_name,
        }));
    }

    async getFields(
        tableName: string,
        schema?: string,
        database?: string,
        tags?: Record<string, string>,
    ): Promise<WarehouseCatalog> {
        const query = `
            SELECT table_catalog,
                   table_schema,
                   table_name,
                   column_name,
                   data_type
            FROM information_schema.columns
            WHERE table_name = $1
            ${schema ? 'AND table_schema = $2' : ''}
            ${database ? 'AND table_catalog = $3' : ''}
        `;
        const values = [tableName];
        if (schema) {
            values.push(schema);
        }
        if (database) {
            values.push(database);
        }
        const { rows } = await this.runQuery(query, tags, undefined, values);

        return this.parseWarehouseCatalog(
            rows,
            mapFieldType,
            getPostgresTimestampDomain,
        );
    }

    parseError(error: pg.DatabaseError, query: string = '') {
        // getErrorLineAndCharPosition is a helper function to get the line and character position of the error
        // NOTE: the database returns "position" which is the count of characters from the start of the query, regardless of newlines
        // this function converts the position to line number and character position
        const getErrorLineAndCharPosition = (
            queryString: string,
            position: string | undefined,
        ) => {
            if (!position) return undefined;
            // convert the position to a number
            const positionNum = parseInt(position, 10);
            // If the position is not a number, return an error message
            if (Number.isNaN(positionNum)) return undefined;
            // Split the queryString into lines
            const lines = queryString.split('\n');
            let currentCharCount = 0;
            // Loop through each line to determine the line number and character position
            for (let i = 0; i < lines.length; i += 1) {
                const line = lines[i];
                const nextCharCount = currentCharCount + line.length + 1; // +1 accounts for the newline character
                // If the position falls within this line
                if (positionNum <= nextCharCount) {
                    const charPosition = positionNum - currentCharCount;
                    return { line: i + 1, charPosition };
                }
                // Update the current character count
                currentCharCount = nextCharCount;
            }
            // If the position is beyond the queryString length, return an error message
            return undefined;
        };
        // do noithing if there is no position returned)
        if (!error?.position) return new WarehouseQueryError(error?.message);
        // The query will look something like this:
        // 'WITH user_sql AS (
        //     SELECT * FROM `lightdash-database-staging`.`e2e_jaffle_shop`.`users`;
        // ) select * from user_sql limit 500';
        // We want to check for the first part of the query, if so strip the first and last lines
        const queryMatch = query.match(/(?:WITH\s+[a-zA-Z_]+\s+AS\s*\()\s*?/i);
        // get the position and line from the position returned from postgres
        const positionObj = getErrorLineAndCharPosition(query, error?.position);
        // do nothing if the line and charNumber cannot be determined
        if (!positionObj) return new WarehouseQueryError(error?.message);
        let lineNumber = positionObj.line;
        const charNumber = positionObj.charPosition;
        // if query match, subtract the number of lines from the line number
        if (queryMatch && lineNumber && lineNumber > 1) {
            lineNumber -= 1;
        }
        // return a new error with the line and character number in data object
        return new WarehouseQueryError(error.message, {
            lineNumber,
            charNumber,
        });
    }
}

// Mimics behaviour in https://github.com/brianc/node-postgres/blob/master/packages/pg-connection-string/index.js
const getSSLConfigFromMode = ({
    sslcert,
    sslkey,
    sslmode,
    sslrootcert,
}: SslConfiguration): PoolConfig['ssl'] => {
    const mode = sslmode || 'prefer';
    const ca = sslrootcert || [
        ...rootCertificates,
        readFileSync(path.resolve(__dirname, './ca-bundle-aws-rds-global.pem')),
    ];
    switch (mode) {
        case 'disable':
            return false;
        case 'prefer':
        case 'require':
        case 'allow':
        case 'verify-ca':
        case 'verify-full':
            return {
                ca,
                cert: sslcert ?? undefined,
                key: sslkey ?? undefined,
                checkServerIdentity: (hostname, cert) => {
                    if (hostname === 'localhost') {
                        // When connecting to localhost, we don't need to validate the server identity
                        // pg library defaults to localhost when connecting via IP address
                        return undefined;
                    }
                    return tls.checkServerIdentity(hostname, cert);
                },
            };
        case 'no-verify':
            return { rejectUnauthorized: false, ca };
        default:
            throw new Error(`Unknown sslmode for postgres: ${mode}`);
    }
};

export class PostgresWarehouseClient extends PostgresClient<CreatePostgresCredentials> {
    constructor(credentials: CreatePostgresCredentials) {
        const ssl = getSSLConfigFromMode(credentials);
        super(credentials, {
            connectionString: `postgres://${encodeURIComponent(
                credentials.user,
            )}:${encodeURIComponent(credentials.password)}@${encodeURIComponent(
                credentials.host,
            )}:${credentials.port}/${encodeURIComponent(credentials.dbname)}`,
            ssl,
        });
    }
}
