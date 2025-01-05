{% macro unit_conversion(value, from_unit, to_unit) %}
    case
        -- Convert meters to miles (1 mile = 1609.34 meters)
        when lower('{{ from_unit }}') = 'meters' and lower('{{ to_unit }}') = 'miles' then {{ value }} * 0.000621371

        -- Convert yards to miles (1 yard = 0.0005681818 miles)
        when lower('{{ from_unit }}') = 'yards' and lower('{{ to_unit }}') = 'miles' then {{ value }} * 0.0005681818

        -- Convert seconds to minutes (1 minute = 60 seconds)
        when lower('{{ from_unit }}') = 'seconds' and lower('{{ to_unit }}') = 'minutes' then {{ value }} / 60

        -- Convert seconds to hours (1 hour = 3600 seconds)
        when lower('{{ from_unit }}') = 'seconds' and lower('{{ to_unit }}') = 'hours' then {{ value }} / 3600

        -- Convert seconds to days (1 day = 86400 seconds)
        when lower('{{ from_unit }}') = 'seconds' and lower('{{ to_unit }}') = 'days' then {{ value }} / 86400

        -- Convert meters to feet (1 meter = 3.28084 feet)
        when lower('{{ from_unit }}') = 'meters' and lower('{{ to_unit }}') = 'feet' then {{ value }} * 3.28084

        -- Convert meters per second to miles per hour (1 mps = 2.23694 mph)
        when lower('{{ from_unit }}') = 'mps' and lower('{{ to_unit }}') = 'mph' then {{ value }} * 2.23694

        else null
    end
{% endmacro %}
