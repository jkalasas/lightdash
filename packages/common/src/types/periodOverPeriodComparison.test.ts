import {
    DimensionType,
    FieldType,
    type Dimension,
    type ItemsMap,
} from './field';
import {
    getPopComparisonConfigKey,
    getPopTimeDimensionCandidates,
    hashPopComparisonConfigKeyToSuffix,
} from './periodOverPeriodComparison';
import { TimeFrames } from './timeFrames';

const timeDim = (
    name: string,
    timeInterval: TimeFrames,
    extras: Partial<Dimension> = {},
): Dimension => ({
    fieldType: FieldType.DIMENSION,
    type: DimensionType.DATE,
    timeInterval,
    name,
    label: name,
    table: 'orders',
    tableLabel: 'Orders',
    sql: 'sql',
    hidden: false,
    ...extras,
});

const regionDim: Dimension = {
    fieldType: FieldType.DIMENSION,
    type: DimensionType.STRING,
    name: 'region',
    label: 'Region',
    table: 'orders',
    tableLabel: 'Orders',
    sql: 'sql',
    hidden: false,
};

const orderDateDay = timeDim('order_date_day', TimeFrames.DAY);
const orderDateMonth = timeDim('order_date_month', TimeFrames.MONTH);
const orderDateYear = timeDim('order_date_year', TimeFrames.YEAR);
const hiddenDay = timeDim('hidden_date_day', TimeFrames.DAY, { hidden: true });
const hourDim = timeDim('created_at_hour', TimeFrames.HOUR);

const itemsMap: ItemsMap = {
    orders_order_date_day: orderDateDay,
    orders_order_date_month: orderDateMonth,
    orders_order_date_year: orderDateYear,
    orders_hidden_date_day: hiddenDay,
    orders_created_at_hour: hourDim,
    orders_region: regionDim,
};

describe('periodOverPeriodComparison helpers', () => {
    test('getPopComparisonConfigKey is deterministic for same inputs', () => {
        const a = getPopComparisonConfigKey({
            timeDimensionId: 'orders_order_date_month',
            granularity: TimeFrames.MONTH,
            periodOffset: 1,
        });
        const b = getPopComparisonConfigKey({
            timeDimensionId: 'orders_order_date_month',
            granularity: TimeFrames.MONTH,
            periodOffset: 1,
        });
        expect(a).toEqual(b);
    });

    test('getPopComparisonConfigKey changes when any field changes', () => {
        const base = getPopComparisonConfigKey({
            timeDimensionId: 'orders_order_date_month',
            granularity: TimeFrames.MONTH,
            periodOffset: 1,
        });

        expect(
            getPopComparisonConfigKey({
                timeDimensionId: 'orders_order_date_month',
                granularity: TimeFrames.MONTH,
                periodOffset: 2,
            }),
        ).not.toEqual(base);

        expect(
            getPopComparisonConfigKey({
                timeDimensionId: 'orders_order_date_week',
                granularity: TimeFrames.MONTH,
                periodOffset: 1,
            }),
        ).not.toEqual(base);

        expect(
            getPopComparisonConfigKey({
                timeDimensionId: 'orders_order_date_month',
                granularity: TimeFrames.WEEK,
                periodOffset: 1,
            }),
        ).not.toEqual(base);
    });

    test('hashPopComparisonConfigKeyToSuffix is deterministic and fixed length', () => {
        const key = getPopComparisonConfigKey({
            timeDimensionId: 'orders_order_date_month',
            granularity: TimeFrames.MONTH,
            periodOffset: 1,
        });

        const a = hashPopComparisonConfigKeyToSuffix(key);
        const b = hashPopComparisonConfigKeyToSuffix(key);

        expect(a).toEqual(b);
        expect(a).toHaveLength(8);
        expect(a).toMatch(/^[0-9a-z]{8}$/);
    });

    test('hashPopComparisonConfigKeyToSuffix differs for different config keys', () => {
        const k1 = getPopComparisonConfigKey({
            timeDimensionId: 'orders_order_date_month',
            granularity: TimeFrames.MONTH,
            periodOffset: 1,
        });
        const k2 = getPopComparisonConfigKey({
            timeDimensionId: 'orders_order_date_month',
            granularity: TimeFrames.MONTH,
            periodOffset: 2,
        });
        expect(k1).not.toEqual(k2);
        expect(hashPopComparisonConfigKeyToSuffix(k1)).not.toEqual(
            hashPopComparisonConfigKeyToSuffix(k2),
        );
    });

    test('getPopComparisonConfigKey is delimiter-safe (timeDimensionId can contain "|")', () => {
        const normal = getPopComparisonConfigKey({
            timeDimensionId: 'orders_order_date_month',
            granularity: TimeFrames.MONTH,
            periodOffset: 1,
        });
        const withPipe = getPopComparisonConfigKey({
            timeDimensionId: 'orders|order_date|month',
            granularity: TimeFrames.MONTH,
            periodOffset: 1,
        });
        expect(withPipe).not.toEqual(normal);
    });

    test('getPopTimeDimensionCandidates returns only selected time dims when any are selected', () => {
        const result = getPopTimeDimensionCandidates({
            itemsMap,
            selectedDimensionIds: ['orders_order_date_year', 'orders_region'],
        });

        expect(result.fromSelected).toBe(true);
        expect(result.dimensions.map((d) => d.name)).toEqual([
            'order_date_year',
        ]);
    });

    test('getPopTimeDimensionCandidates falls back to explore-wide time dims when none are selected', () => {
        const result = getPopTimeDimensionCandidates({
            itemsMap,
            selectedDimensionIds: ['orders_region'],
        });

        expect(result.fromSelected).toBe(false);
        expect(result.dimensions.map((d) => d.name)).toEqual([
            'order_date_day',
            'order_date_month',
            'order_date_year',
        ]);
    });

    test('getPopTimeDimensionCandidates excludes hidden and unsupported grains from explore-wide list', () => {
        const result = getPopTimeDimensionCandidates({
            itemsMap,
            selectedDimensionIds: [],
        });

        expect(result.dimensions.map((d) => d.name)).not.toContain(
            'hidden_date_day',
        );
        expect(result.dimensions.map((d) => d.name)).not.toContain(
            'created_at_hour',
        );
        expect(result.dimensions.map((d) => d.name)).not.toContain('region');
    });

    test('getPopTimeDimensionCandidates includes a selected hidden time dim', () => {
        const result = getPopTimeDimensionCandidates({
            itemsMap,
            selectedDimensionIds: ['orders_hidden_date_day'],
        });

        expect(result.fromSelected).toBe(true);
        expect(result.dimensions.map((d) => d.name)).toEqual([
            'hidden_date_day',
        ]);
    });
});
