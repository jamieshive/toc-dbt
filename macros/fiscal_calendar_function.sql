{# This function takes in a column of dates and builds a calendar that assigns each date to its fiscal period information #}

{# This function is currently used in the calendar table #}

{% macro get_fiscal_calendar(column_name) %}

  {{ column_name }} as d_day,
  date_part(dayofweek,d_day)+1 as day_of_week, {# assigns each day a number between 1 and 7 to denote its day of the week #}

    case 
        when d_day < '12-26-2011' 
          then {{ dbt_utils.datediff("'2010-12-30'", 'd_day', 'day') }}
      when d_day < '12-31-2012' 
          then {{ dbt_utils.datediff("'2011-12-26'", 'd_day', 'day') }}
      when d_day < '12-30-2013' 
          then {{ dbt_utils.datediff("'2012-12-31'", 'd_day', 'day') }}
      when d_day < '12-29-2014' 
          then {{ dbt_utils.datediff("'2013-12-30'", 'd_day', 'day') }}
      when d_day < '12-28-2015' 
          then {{ dbt_utils.datediff("'2014-12-29'", 'd_day', 'day') }}
      when d_day < '12-29-2016' 
          then {{ dbt_utils.datediff("'2015-12-28'", 'd_day', 'day') }}
      when d_day < '12-28-2017' 
          then {{ dbt_utils.datediff("'2016-12-29'", 'd_day', 'day') }}
      when d_day < '12-27-2018' 
          then {{ dbt_utils.datediff("'2017-12-28'", 'd_day', 'day') }}
      when d_day < '12-26-2019' 
          then {{ dbt_utils.datediff("'2018-12-27'", 'd_day', 'day') }}
      when d_day < '12-31-2020' 
          then {{ dbt_utils.datediff("'2019-12-26'", 'd_day', 'day') }}
      when d_day < '12-30-2021' 
          then {{ dbt_utils.datediff("'2020-12-31'", 'd_day', 'day') }}
      when d_day < '12-29-2022' 
          then {{ dbt_utils.datediff("'2021-12-30'", 'd_day', 'day') }}
      else {{ dbt_utils.datediff("'2022-12-29'", 'd_day', 'day') }}
    end as num_of_days_counter, {# assigns each day of each year, starting in 2011, a number between 1 and 365 to denote its day of year #}
                                {# This will likely need to be updated in 2023 #}
    case 
        when num_of_days_counter < '28' 
          then num_of_days_counter+1 
        when num_of_days_counter > '27' and num_of_days_counter < '364' 
          then mod(num_of_days_counter,28)+1 
        when num_of_days_counter > '363'
          then '29'
    end as day_of_period, {# assigns each day a number between 1 and 28 (to be used in determining fiscal weeks) #}

    case 
        when num_of_days_counter < '371'
          then num_of_days_counter+1 
        else mod(num_of_days_counter,(28*13))+1 
    end as day_of_year, {# assigns each day a number between 1 and 13 (to be used in determining fiscal periods) #}

    case 
        when day_of_period < 8 
          then 'W1' 
        when day_of_period < 15 
          then 'W2'
        when day_of_period < 22 
          then 'W3'
        when day_of_period < 29 
          then 'W4'
        else 'W4' 
    end as week_of_period, {# assigns days to their fiscal weeks #}

    case 
        when day_of_year < (28*1)+1 
          then 'P01'
        when day_of_year < (28*2)+1 
          then 'P02'
        when day_of_year < (28*3)+1 
          then 'P03'
        when day_of_year < (28*4)+1 
          then 'P04'
        when day_of_year < (28*5)+1 
          then 'P05'
        when day_of_year < (28*6)+1 
          then 'P06'
        when day_of_year < (28*7)+1 
          then 'P07'
        when day_of_year < (28*8)+1 
          then 'P08'
        when day_of_year < (28*9)+1 
          then 'P09'
        when day_of_year < (28*10)+1 
          then 'P10'
        when day_of_year < (28*11)+1 
          then 'P11'
        when day_of_year < (28*12)+1 
          then 'P12'
        else 'P13' 
    end as fiscal_period, {# assigns days to their fiscal periods #}
  
    case 
        when extract(year from d_day) != extract(year from d_day+9) and day_of_year < 7
          then extract(year from d_day)+1 
        else extract(year from d_day) 
    end as year, {# assigns fiscal years to dates, where some fiscal years begin in the prior calendar year #}
                 {# ex. 2021P01 began on December 27th, 2020 #}

  ( year || fiscal_period ) as year_period, {# assigns fiscal year and period to date #}
  ( year || fiscal_period ||  week_of_period ) as year_period_week, {# assigns fiscal year, period, and week to date #}
  ( year || fiscal_period ||  week_of_period || day_of_week) as year_period_week_day {# assigns fiscal year, period, week, and day to date#}
{% endmacro %}
