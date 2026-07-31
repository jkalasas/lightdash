/* eslint-disable prefer-arrow-callback, func-names */
import * as pg from 'pg';
import type { Mock } from 'vitest';
import {
    PostgresSqlBuilder,
    postgresStreamConcurrencyForTests,
    PostgresWarehouseClient,
} from './PostgresWarehouseClient';
import {
    columns,
    credentials,
    queryColumnsMock,
} from './PostgresWarehouseClient.mock';
import {
    config,
    expectedFields,
    expectedRow,
    expectedWarehouseSchemaWithNaiveTimestamp,
} from './WarehouseClient.mock';

const isSessionSetupQuery = (sql: string) =>
    sql.startsWith('SET statement_timeout') || sql.startsWith('SET timezone');

const isVersionQuery = (sql: string) =>
    sql.toLowerCase().includes('select version()');

const isCatalogQuery = (sql: string) =>
    sql.toLowerCase().includes('information_schema.columns');

vi.mock('pg', async () => ({
    ...(await vi.importActual<{ default: typeof import('pg') }>('pg')).default,
    Pool: vi.fn(function () {
        return {
            connect: vi.fn(async () => ({
                query: vi.fn(async (sql: string) => {
                    if (isSessionSetupQuery(sql)) {
                        return { rows: [], fields: [] };
                    }
                    return {
                        rows: [expectedRow],
                        fields: queryColumnsMock,
                    };
                }),
                on: vi.fn(),
                release: vi.fn(),
            })),
            end: vi.fn(async () => undefined),
            on: vi.fn(),
        };
    }),
}));

describe('PostgresWarehouseClient', () => {
    it('expect query rows', async () => {
        const warehouse = new PostgresWarehouseClient(credentials);
        const results = await warehouse.runQuery('fake sql');
        expect(results.fields).toEqual(expectedFields);
        expect(results.rows[0]).toEqual(expectedRow);
    });
    it('expect schema with postgres types mapped to dimension types', async () => {
        const warehouse = new PostgresWarehouseClient(credentials);
        (pg.Pool as unknown as Mock)
            .mockImplementationOnce(function () {
                return {
                    connect: vi.fn(async () => ({
                        query: vi.fn(async (sql: string) => {
                            if (isSessionSetupQuery(sql)) {
                                return { rows: [], fields: [] };
                            }
                            if (isVersionQuery(sql)) {
                                return {
                                    rows: [{ version: 'PostgreSQL 15.4' }],
                                    fields: [],
                                };
                            }
                            return { rows: [], fields: [] };
                        }),
                        on: vi.fn(),
                        release: vi.fn(),
                    })),
                    end: vi.fn(async () => undefined),
                    on: vi.fn(),
                };
            })
            .mockImplementationOnce(function () {
                return {
                    connect: vi.fn(async () => ({
                        query: vi.fn(async (sql: string) => {
                            if (isSessionSetupQuery(sql)) {
                                return { rows: [], fields: [] };
                            }
                            if (isCatalogQuery(sql)) {
                                return {
                                    rows: columns,
                                    fields: [],
                                };
                            }
                            return { rows: [], fields: [] };
                        }),
                        on: vi.fn(),
                        release: vi.fn(),
                    })),
                    end: vi.fn(async () => undefined),
                    on: vi.fn(),
                };
            });
        expect(await warehouse.getCatalog(config)).toEqual(
            expectedWarehouseSchemaWithNaiveTimestamp,
        );
    });
    it('expect empty catalog when dbt project has no references', async () => {
        const warehouse = new PostgresWarehouseClient(credentials);
        expect(await warehouse.getCatalog([])).toEqual({});
    });
});

describe('PostgresWarehouseClient statement timeout', () => {
    const mockPoolWithQuery = (queryMock: Mock) => {
        const releaseMock = vi.fn();
        const endMock = vi.fn(async () => undefined);
        const client = {
            query: queryMock,
            on: vi.fn(),
            release: releaseMock,
        };
        (pg.Pool as unknown as Mock).mockImplementationOnce(function () {
            return {
                connect: vi.fn(async () => client),
                end: endMock,
                on: vi.fn(),
            };
        });
        return { releaseMock, endMock, client };
    };

    const respondingQueryMock = () =>
        vi.fn(async (sql: string) => {
            if (isSessionSetupQuery(sql)) {
                return { rows: [], fields: [] };
            }
            return {
                rows: [expectedRow],
                fields: queryColumnsMock,
            };
        });

    const stringQueriesFrom = (queryMock: Mock) =>
        queryMock.mock.calls
            .map((call) => call[0])
            .filter((arg): arg is string => typeof arg === 'string');

    afterEach(() => {
        vi.useRealTimers();
    });

    it('sets a server-side statement_timeout using the 9-minute default ceiling', async () => {
        const queryMock = respondingQueryMock();
        mockPoolWithQuery(queryMock);
        const warehouse = new PostgresWarehouseClient(credentials);
        await warehouse.runQuery('select 1');
        const sessionStatement = stringQueriesFrom(queryMock).find((arg) =>
            arg.includes('SET statement_timeout'),
        );
        expect(sessionStatement).toContain('SET statement_timeout = 540000');
    });

    it('honors a configured timeoutSeconds for the statement_timeout', async () => {
        const queryMock = respondingQueryMock();
        mockPoolWithQuery(queryMock);
        const warehouse = new PostgresWarehouseClient({
            ...credentials,
            timeoutSeconds: 120,
        });
        await warehouse.runQuery('select 1');
        const sessionStatement = stringQueriesFrom(queryMock).find((arg) =>
            arg.includes('SET statement_timeout'),
        );
        expect(sessionStatement).toContain('SET statement_timeout = 120000');
    });

    it('runs a one-shot query without BEGIN/COMMIT and soft-releases on success', async () => {
        const queryMock = respondingQueryMock();
        const { releaseMock, endMock } = mockPoolWithQuery(queryMock);
        const warehouse = new PostgresWarehouseClient(credentials);
        await warehouse.runQuery('select 1');

        const stringQueries = stringQueriesFrom(queryMock);

        expect(stringQueries[0]).toContain('SET statement_timeout');
        expect(stringQueries).toContain('select 1');
        expect(stringQueries).not.toContain('BEGIN');
        expect(stringQueries).not.toContain('COMMIT');
        expect(stringQueries).not.toContain('ROLLBACK');

        expect(releaseMock).toHaveBeenCalledTimes(1);
        expect(releaseMock.mock.calls[0]).toEqual([]);
        expect(endMock).toHaveBeenCalledTimes(1);
    });

    it('discards the client when the one-shot query fails', async () => {
        const queryMock = vi.fn(async (sql: string) => {
            if (isSessionSetupQuery(sql)) {
                return { rows: [], fields: [] };
            }
            throw new Error('relation "missing" does not exist');
        });
        const { releaseMock, endMock } = mockPoolWithQuery(queryMock);
        const warehouse = new PostgresWarehouseClient(credentials);

        await expect(warehouse.runQuery('select 1')).rejects.toThrow(
            'relation "missing" does not exist',
        );

        const stringQueries = stringQueriesFrom(queryMock);
        expect(stringQueries).not.toContain('BEGIN');
        expect(stringQueries).not.toContain('ROLLBACK');
        expect(stringQueries).not.toContain('COMMIT');

        expect(releaseMock).toHaveBeenCalledTimes(1);
        expect(releaseMock.mock.calls[0][0]).toBeInstanceOf(Error);
        expect(releaseMock.mock.calls[0][0].message).toContain(
            'discarding client',
        );
        expect(endMock).toHaveBeenCalledTimes(1);
    });

    it('destroys the client without issuing ROLLBACK when the query fails', async () => {
        const queryMock = vi.fn(async (sql: string) => {
            if (isSessionSetupQuery(sql)) {
                return { rows: [], fields: [] };
            }
            throw new Error('query failed');
        });
        const client = {
            query: queryMock,
            on: vi.fn(),
            release: vi.fn(),
            connection: {
                stream: {
                    destroyed: false,
                    destroy: vi.fn(
                        function destroy(this: { destroyed: boolean }) {
                            this.destroyed = true;
                        },
                    ),
                },
            },
        };
        Reflect.set(client, '_queryable', true);
        const endMock = vi.fn(async () => undefined);
        (pg.Pool as unknown as Mock).mockImplementationOnce(function () {
            return {
                connect: vi.fn(async () => client),
                end: endMock,
                on: vi.fn(),
            };
        });
        const warehouse = new PostgresWarehouseClient(credentials);

        await expect(warehouse.runQuery('select 1')).rejects.toThrow(
            'query failed',
        );

        expect(stringQueriesFrom(queryMock)).not.toContain('ROLLBACK');
        expect(client.connection.stream.destroy).toHaveBeenCalled();
        expect(Reflect.get(client, '_queryable')).toBe(false);
        expect(client.release).toHaveBeenCalledTimes(1);
        expect(client.release.mock.calls[0][0]).toBeInstanceOf(Error);
        expect(endMock).toHaveBeenCalledTimes(1);
    });

    it('rejects with a timeout error when a query stalls past the client backstop', async () => {
        vi.useFakeTimers();
        const queryMock = vi.fn((sql: string) => {
            if (isSessionSetupQuery(sql)) {
                return Promise.resolve({ rows: [], fields: [] });
            }
            return new Promise(() => {});
        });
        const { releaseMock, endMock } = mockPoolWithQuery(queryMock);
        const warehouse = new PostgresWarehouseClient(credentials);
        const resultPromise = warehouse.runQuery('select pg_sleep(9999)');
        await Promise.all([
            expect(resultPromise).rejects.toThrow('Query timed out after 570s'),
            vi.advanceTimersByTimeAsync(570 * 1000 + 1000),
        ]);

        expect(stringQueriesFrom(queryMock)).not.toContain('ROLLBACK');
        expect(releaseMock).toHaveBeenCalledTimes(1);
        expect(releaseMock.mock.calls[0][0]).toBeInstanceOf(Error);
        expect(endMock).toHaveBeenCalledTimes(1);
    });

    it('invokes the stream callback in chunks for multi-row results', async () => {
        const rows = Array.from({ length: 1200 }, (_, i) => ({
            ...expectedRow,
            id: i,
        }));
        const queryMock = vi.fn(async (sql: string) => {
            if (isSessionSetupQuery(sql)) {
                return { rows: [], fields: [] };
            }
            return { rows, fields: queryColumnsMock };
        });
        mockPoolWithQuery(queryMock);
        const warehouse = new PostgresWarehouseClient(credentials);

        const callbackSizes: number[] = [];
        await warehouse.streamQuery(
            'select * from big',
            ({ rows: chunk }) => {
                callbackSizes.push(chunk.length);
            },
            {},
        );

        expect(callbackSizes).toEqual([500, 500, 200]);
    });
});

describe('PostgresWarehouseClient concurrency', () => {
    afterEach(() => {
        postgresStreamConcurrencyForTests.reset();
        vi.useRealTimers();
    });

    it('queues streams so concurrent open pools never exceed the limit', async () => {
        postgresStreamConcurrencyForTests.setLimit(1);

        let inFlightPools = 0;
        let maxInFlightPools = 0;

        (pg.Pool as unknown as Mock).mockImplementation(function () {
            inFlightPools += 1;
            maxInFlightPools = Math.max(maxInFlightPools, inFlightPools);
            return {
                connect: vi.fn(async () => ({
                    query: vi.fn(async (sql: string) => {
                        if (isSessionSetupQuery(sql)) {
                            return { rows: [], fields: [] };
                        }
                        await new Promise((resolve) => {
                            setTimeout(resolve, 30);
                        });
                        return {
                            rows: [expectedRow],
                            fields: queryColumnsMock,
                        };
                    }),
                    on: vi.fn(),
                    release: vi.fn(),
                })),
                end: vi.fn(async () => {
                    inFlightPools -= 1;
                }),
                on: vi.fn(),
            };
        });

        const warehouse = new PostgresWarehouseClient(credentials);
        await Promise.all([
            warehouse.runQuery('select 1'),
            warehouse.runQuery('select 2'),
            warehouse.runQuery('select 3'),
        ]);

        expect(maxInFlightPools).toBe(1);
        expect(postgresStreamConcurrencyForTests.getActiveCount()).toBe(0);
        expect(postgresStreamConcurrencyForTests.getWaitingCount()).toBe(0);
    });
});

describe('PostgresSqlBuilder escaping', () => {
    const postgresSqlBuilder = new PostgresSqlBuilder();

    test('Should not escape regular characters', () => {
        expect(postgresSqlBuilder.escapeString('%')).toBe('%');
        expect(postgresSqlBuilder.escapeString('_')).toBe('_');
        expect(postgresSqlBuilder.escapeString('?')).toBe('?');
        expect(postgresSqlBuilder.escapeString('!')).toBe('!');
        expect(postgresSqlBuilder.escapeString('credit_card')).toBe(
            'credit_card',
        );
    });

    test('Should escape single quotes in postgres', () => {
        expect(postgresSqlBuilder.escapeString("single'quote")).toBe(
            "single''quote",
        );
    });

    test('Should escape backslashes and quotes in postgres', () => {
        expect(postgresSqlBuilder.escapeString("\\') OR (1=1) --")).toBe(
            "\\\\'') OR (1=1) ",
        );
    });

    test('Should handle SQL injection attempts', () => {
        const maliciousInput = "'; DROP TABLE users; --";
        const escaped = postgresSqlBuilder.escapeString(maliciousInput);
        expect(escaped).toBe("''; DROP TABLE users; ");

        const anotherMaliciousInput = "' OR '1'='1";
        const anotherEscaped = postgresSqlBuilder.escapeString(
            anotherMaliciousInput,
        );
        expect(anotherEscaped).toBe("'' OR ''1''=''1");
    });
});
