DO $$
DECLARE
    col_name text;
    column_type text;
    num_uniques int;
    uniques text;
    num_nulls int;
    num_values int;
    num_empty int;
    longest_string int;

BEGIN
    -- Loop through all columns except the numeric and date columns
    FOR col_name, column_type IN
        SELECT column_name, data_type
        FROM information_schema.columns
        WHERE table_name = 'medicine_sales'
        AND table_schema = 'public'
        AND column_name NOT IN ('id', 'year', 'imported_at')
    LOOP
        -- Count unique values for the current column
        EXECUTE format('SELECT COUNT(DISTINCT %I) FROM public.medicine_sales', col_name)
        INTO num_uniques;

        -- Count NULL values for the current column
        EXECUTE format('SELECT COUNT(*) FROM public.medicine_sales WHERE %I IS NULL', col_name)
        INTO num_nulls;

        -- Count non-NULL values for the current column
        EXECUTE format('SELECT COUNT(*) FROM public.medicine_sales WHERE %I IS NOT NULL', col_name)
        INTO num_values;

        -- Count empty values for the current column
        EXECUTE format('SELECT COUNT(*) FROM public.medicine_sales WHERE %I = ''''', col_name)
        INTO num_empty;

        -- Calculate the longest string length for the current column
        EXECUTE format('SELECT MAX(LENGTH(%I)) FROM public.medicine_sales', col_name)
        INTO longest_string;

        -- Handle case where unique values are less than 10
        IF num_uniques < 10 THEN
            EXECUTE format('
                SELECT STRING_AGG(DISTINCT %I::TEXT, '', '')
                FROM public.medicine_sales
            ', col_name) INTO uniques;
        ELSE
            uniques := 'More than 10 unique values';
        END IF;

        -- Output the result
        RAISE NOTICE 'Column: %, Type: %, Unique: %, NULL: %, Non-NULL: %, Empty: %, Longest String Length: %, Values: %',
                     col_name, column_type, num_uniques, num_nulls, num_values, num_empty, longest_string, uniques;

    END LOOP;
END $$;
