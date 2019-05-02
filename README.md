# Dataflows Resource write to db normalized

This library provides some dataflows processing for normalizing a resource.

It has special support for storing normalized data into DB tables.

## What is normalization?

In short, it is the process of reducing duplication in a dataset.

More can be read about this concept [here](https://en.wikipedia.org/wiki/Database_normalization).

## Example

Let's take, as an example, this world cities dataset (we shall call it the *fact* resource):

```python
from dataflows import Flow, load, printer

Flow(
    load('https://datahub.io/core/world-cities/r/world-cities.csv', name='cities'),
    printer(num_rows=1)
).process()
```

*cities:*
|#  |name          |country |subcountry        |geonameid
|-----|----------------|----------|------------------|-----------
  |1    |les Escaldes    |Andorra   |Escaldes-Engordany    |3040051
  |2    |Andorra la Vella|Andorra   |Andorra la Vella      |3041563
  |...
  |23018|**Chitungwiza**     |**Zimbabwe**  |**Harare**                |1106542



It seems that the `country` and `subcountry` columns are quite repetitive - let's extract them into a separate, deduplicated resource (we will call that a *dimension* resource).

To do that we use the `normalize` processor.

This processor receives a single resource name, and a list of `NormGroup` instances. Each of these groups specifies one new *dimension* resource to be extracted and deduplicated.

Let's see it in action:


```python
from dataflows_normalize import normalize, NormGroup

Flow(
    load('https://datahub.io/core/world-cities/r/world-cities.csv', name='cities'),
    normalize([
       NormGroup(['country', 'subcountry'], 'country_id', 'id') 
    ], resource='cities'),
    printer()
).process()
```

*cities:*
  |#    |name               |geonameid  |country_id
  |-----|-----------------|-----------|------------
  |1    |les Escaldes         |3040051           |0
  |2    |Andorra la Vella     |3041563           |1
  |3    |Umm al Qaywayn        |290594           |2
  |4    |Ras al-Khaimah        |291074           |3
  |5    |Khawr Fakkān          |291696           |4
  |...
  |23014|Bulawayo              |894701        |2677
  |23015|Bindura               |895061        |2678
  |23016|Beitbridge            |895269        |2679
  |23017|Epworth              |1085510        |2676
  |23018|**Chitungwiza**          |1106542        |**2676**

*cities_country_id:*
  |#            |id|country            |subcountry
  |----|-----------|-------------------|----------------------
  |1            |30|Afghanistan        |Badakhshan
  |2            |27|Afghanistan        |Badghis
  |3            |21|Afghanistan        |Balkh
  |4            |33|Afghanistan        |Bāmīān
  |5            |31|Afghanistan        |Farah
  |6            |19|Afghanistan        |Faryab
  |7            |28|Afghanistan        |Ghaznī
  |8            |13|Afghanistan        |Ghowr
  |9            |22|Afghanistan        |Helmand
  |10           |11|Afghanistan        |Herat
  |...
  |2671       |2677|Zimbabwe           |Bulawayo
  |2672       |**2676**|**Zimbabwe**           |**Harare**
  |2673       |2673|Zimbabwe           |Manicaland
  |2674       |2678|Zimbabwe           |Mashonaland Central
  |2675       |2675|Zimbabwe           |Mashonaland East
  |2676       |2674|Zimbabwe           |Mashonaland West
  |2677       |2670|Zimbabwe           |Masvingo
  |2678       |2671|Zimbabwe           |Matabeleland North
  |2679       |2679|Zimbabwe           |Matabeleland South
  |2680       |2672|Zimbabwe           |Midlands

If we follow the last line in the dataset (`Chitungwiza`), we can see that an entry for its region (`Zimbabwe/Harare`) was created with id `2676`, and that id was added to the original row instead of the original values.

**How much did we gain?**

The original CSV file has a size of 895,586 bytes.

If we save the two new resources as CSVs, we would get

542,299 bytes for the *fact* resource and 68,023 for the regions *dimension* resource - a total of 610,322 bytes (or a reduction of 31% in size).

Not only this helps with size, it also improves greatly DB performance to store data in normalized form.

## DB Normalization

Running similar code to above, only using `normalize_to_db` will do the following:
- Load existing values from database *dimension* tables (in case these tables exist)
- Normalize the input data, and split into *fact* and *dimension* resources
- Update the DB tables with new values, while reusing existing references

The main difference in usage from `normalize` is that the names of DB tables are provided.


```python
from dataflows_normalize import normalize_to_db, NormGroup

Flow(
    load('https://datahub.io/core/world-cities/r/world-cities.csv', name='cities'),
    normalize_to_db(
        [
            NormGroup(['country', 'subcountry'], 'country_id', 'id', db_table='countries_db_table') 
        ], 
        'cities_db_table', 'cities',
        db_connection_str='...'
    ),
).process()
```
