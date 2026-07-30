/* eslint-disable prefer-arrow-callback, func-names */
import * as pg from 'pg';
import { PassThrough } from 'stream';
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

vi.mock('pg', async () => ({
    ...(await vi.importActual<{ default: typeof import('pg') }>('pg')).default,
    Pool: vi.fn(function () {
        return {
            connect: vi.fn((callback) => {
                callback(
                    null,
                    {
                        query: vi.fn((arg: unknown) => {
                            // Transaction + session statements (BEGIN/COMMIT/
                            // ROLLBACK, SET statement_timeout / timezone) are
                            // issued as plain string queries and must return a
                            // thenable, not a stream.
                            if (typeof arg === 'string') {
                                return Promise.resolve({
                                    rows: [],
                                    fields: [],
                                });
                            }
                            const mockedStream = new PassThrough();
                            setTimeout(() => {
                                mockedStream.emit('data', {
                                    row: expectedRow,
                                    fields: queryColumnsMock,
                                });
                                mockedStream.end();
                            }, 100);
                            return mockedStream;
                        }),
                        on: vi.fn(async () => undefined),
                    },
                    vi.fn(),
                );
            }),
            end: vi.fn(async () => undefined),
            on: vi.fn(async () => undefined),
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
                    connect: vi.fn((callback) => {
                        callback(
                            null,
                            {
                                query: vi.fn((arg: unknown) => {
                                    if (typeof arg === 'string') {
                                        return Promise.resolve({
                                            rows: [],
                                            fields: [],
                                        });
                                    }
                                    const mockedStream = new PassThrough();
                                    setTimeout(() => {
                                        mockedStream.emit('data', {
                                            row: { version: 'PostgreSQL 15.4' },
                                            fields: [],
                                        });
                                        mockedStream.end();
                                    }, 100);
                                    return mockedStream;
                                }),
                                on: vi.fn(async () => undefined),
                            },
                            vi.fn(),
                        );
                    }),
                    end: vi.fn(async () => undefined),
                    on: vi.fn(async () => undefined),
                };
            })
            .mockImplementationOnce(function () {
                return {
                    connect: vi.fn((callback) => {
                        callback(
                            null,
                            {
                                query: vi.fn((arg: unknown) => {
                                    if (typeof arg === 'string') {
                                        return Promise.resolve({
                                            rows: [],
                                            fields: [],
                                        });
                                    }
                                    const mockedStream = new PassThrough();
                                    setTimeout(() => {
                                        columns.forEach((column) => {
                                            mockedStream.emit('data', {
                                                row: column,
                                                fields: [],
                                            });
                                        });
                                        mockedStream.end();
                                    }, 100);
                                    return mockedStream;
                                }),
                                on: vi.fn(async () => undefined),
                            },
                            vi.fn(),
                        );
                    }),
                    end: vi.fn(async () => undefined),
                    on: vi.fn(async () => undefined),
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
        (pg.Pool as unknown as Mock).mockImplementationOnce(function () {
            return {
                connect: vi.fn((callback) => {
                    callback(
                        null,
                        { query: queryMock, on: vi.fn() },
                        releaseMock,
                    );
                }),
                end: endMock,
                on: vi.fn(),
            };
        });
        return { releaseMock, endMock };
    };

    const respondingQueryMock = () =>
        vi.fn((arg: unknown) => {
            if (typeof arg === 'string') {
                return Promise.resolve({ rows: [], fields: [] });
            }
            const stream = new PassThrough();
            setTimeout(() => {
                stream.emit('data', {
                    row: expectedRow,
                    fields: queryColumnsMock,
                });
                stream.end();
            }, 10);
            return stream;
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

    it('wraps the cursor stream in BEGIN...COMMIT and soft-releases on success', async () => {
        const queryMock = respondingQueryMock();
        const { releaseMock, endMock } = mockPoolWithQuery(queryMock);
        const warehouse = new PostgresWarehouseClient(credentials);
        await warehouse.runQuery('select 1');

        const stringQueries = stringQueriesFrom(queryMock);

        expect(stringQueries[0]).toBe('BEGIN');
        expect(stringQueries[1]).toContain('SET statement_timeout');
        expect(stringQueries.at(-1)).toBe('COMMIT');
        expect(stringQueries).not.toContain('ROLLBACK');

        const streamCallIndex = queryMock.mock.calls.findIndex(
            (call) => typeof call[0] !== 'string',
        );
        const beginIndex = queryMock.mock.calls.findIndex(
            (call) => call[0] === 'BEGIN',
        );
        const commitIndex = queryMock.mock.calls.findIndex(
            (call) => call[0] === 'COMMIT',
        );
        expect(beginIndex).toBeGreaterThanOrEqual(0);
        expect(streamCallIndex).toBeGreaterThan(beginIndex);
        expect(commitIndex).toBeGreaterThan(streamCallIndex);

        expect(releaseMock).toHaveBeenCalledTimes(1);
        expect(releaseMock.mock.calls[0]).toEqual([]);
        expect(endMock).toHaveBeenCalledTimes(1);
    });

    it('rolls back and destroys the client when the cursor stream fails', async () => {
        const queryMock = vi.fn((arg: unknown) => {
            if (typeof arg === 'string') {
                return Promise.resolve({ rows: [], fields: [] });
            }
            const stream = new PassThrough();
            setTimeout(() => {
                stream.destroy(new Error('portal "C_1" does not exist'));
            }, 10);
            return stream;
        });
        const { releaseMock, endMock } = mockPoolWithQuery(queryMock);
        const warehouse = new PostgresWarehouseClient(credentials);

        await expect(warehouse.runQuery('select 1')).rejects.toThrow(
            'portal "C_1" does not exist',
        );

        const stringQueries = stringQueriesFrom(queryMock);

        expect(stringQueries[0]).toBe('BEGIN');
        expect(stringQueries).toContain('ROLLBACK');
        expect(stringQueries).not.toContain('COMMIT');

        expect(releaseMock).toHaveBeenCalledTimes(1);
        expect(releaseMock.mock.calls[0][0]).toBeInstanceOf(Error);
        expect(releaseMock.mock.calls[0][0].message).toContain(
            'discarding client',
        );
        expect(endMock).toHaveBeenCalledTimes(1);
    });

    it('destroys the client when COMMIT fails after a successful stream', async () => {
        const queryMock = vi.fn((arg: unknown) => {
            if (typeof arg === 'string') {
                if (arg === 'COMMIT') {
                    return Promise.reject(new Error('commit failed'));
                }
                return Promise.resolve({ rows: [], fields: [] });
            }
            const stream = new PassThrough();
            setTimeout(() => {
                stream.emit('data', {
                    row: expectedRow,
                    fields: queryColumnsMock,
                });
                stream.end();
            }, 10);
            return stream;
        });
        const { releaseMock, endMock } = mockPoolWithQuery(queryMock);
        const warehouse = new PostgresWarehouseClient(credentials);

        await expect(warehouse.runQuery('select 1')).rejects.toThrow(
            'commit failed',
        );

        expect(stringQueriesFrom(queryMock)).toContain('COMMIT');
        expect(releaseMock).toHaveBeenCalledTimes(1);
        expect(releaseMock.mock.calls[0][0]).toBeInstanceOf(Error);
        expect(endMock).toHaveBeenCalledTimes(1);
    });

    it('destroys the client and ends the pool when ROLLBACK hangs', async () => {
        vi.useFakeTimers();
        const queryMock = vi.fn((arg: unknown) => {
            if (typeof arg === 'string') {
                if (arg === 'ROLLBACK') {
                    return new Promise(() => {});
                }
                return Promise.resolve({ rows: [], fields: [] });
            }
            const stream = new PassThrough();
            setTimeout(() => {
                stream.destroy(new Error('portal "C_1" does not exist'));
            }, 10);
            return stream;
        });
        const { releaseMock, endMock } = mockPoolWithQuery(queryMock);
        const warehouse = new PostgresWarehouseClient(credentials);

        const resultPromise = warehouse.runQuery('select 1');
        await Promise.all([
            expect(resultPromise).rejects.toThrow(
                'portal "C_1" does not exist',
            ),
            (async () => {
                await vi.advanceTimersByTimeAsync(20);
                await vi.advanceTimersByTimeAsync(1500);
            })(),
        ]);

        expect(stringQueriesFrom(queryMock)).toContain('ROLLBACK');
        expect(releaseMock).toHaveBeenCalledTimes(1);
        expect(releaseMock.mock.calls[0][0]).toBeInstanceOf(Error);
        expect(endMock).toHaveBeenCalledTimes(1);
    });

    it('rejects with a timeout error when a query stalls past the client backstop', async () => {
        vi.useFakeTimers();
        const queryMock = vi.fn((arg: unknown) => {
            if (typeof arg === 'string') {
                return Promise.resolve({ rows: [], fields: [] });
            }
            return new PassThrough();
        });
        const { releaseMock, endMock } = mockPoolWithQuery(queryMock);
        const warehouse = new PostgresWarehouseClient(credentials);
        const resultPromise = warehouse.runQuery('select pg_sleep(9999)');
        await Promise.all([
            expect(resultPromise).rejects.toThrow('Query timed out after 570s'),
            vi.advanceTimersByTimeAsync(570 * 1000 + 1000),
        ]);

        expect(stringQueriesFrom(queryMock)).toContain('ROLLBACK');
        expect(releaseMock).toHaveBeenCalledTimes(1);
        expect(releaseMock.mock.calls[0][0]).toBeInstanceOf(Error);
        expect(endMock).toHaveBeenCalledTimes(1);
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
                connect: vi.fn((callback) => {
                    callback(
                        null,
                        {
                            query: vi.fn((arg: unknown) => {
                                if (typeof arg === 'string') {
                                    return Promise.resolve({
                                        rows: [],
                                        fields: [],
                                    });
                                }
                                const stream = new PassThrough();
                                setTimeout(() => {
                                    stream.emit('data', {
                                        row: expectedRow,
                                        fields: queryColumnsMock,
                                    });
                                    stream.end();
                                }, 30);
                                return stream;
                            }),
                            on: vi.fn(),
                        },
                        vi.fn(),
                    );
                }),
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
        // Test with a typical SQL injection pattern
        const maliciousInput = "'; DROP TABLE users; --";
        const escaped = postgresSqlBuilder.escapeString(maliciousInput);
        expect(escaped).toBe("''; DROP TABLE users; ");

        // Test with another common SQL injection pattern
        const anotherMaliciousInput = "' OR '1'='1";
        const anotherEscaped = postgresSqlBuilder.escapeString(
            anotherMaliciousInput,
        );
        expect(anotherEscaped).toBe("'' OR ''1''=''1");
    });
});
