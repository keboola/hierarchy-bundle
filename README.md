Not Working!
==================

Hierarchy Reconstruction 
==================

Hierarchy Reconstruction application builds a tree from an ancestor relationship (parent id), sorts the tree and returns the tree position (position from top, depth from left) of every node of the tree. The output of this transformation are 4 new columns

- **root** - ID of the item that is root of the hierarchy thread
- **position** - number of items that are above the items in all hierarchy
- **position_relative** - number of items, that are above under the same root
- **depth** - nesting level of the item

If timeDelta is turned on, then additional 2 columns are added
 
- **time_delta** - number of seconds between an item on the same or lower level of depth (a previous sibling or a parent if there is no previous sibilng)
- **time_delta_runsum** - number of seconds from the root of the current hierarchy branch

This transformation uses MySQL as its backend and scales lineary.

Configuration
-------------------

Both the output file is named **destination.csv**. The application takes the following parameters - see the screenshot for sample configuration:

- **source** - source table in Storage API.
- **destination** - destination table in Storage API.
- **columns.id** - primary key of the source table.
- **columns.sort** - column that the items will be sorted by.
- **columns.parent** - column storing the parent id.
- **timeDelta** (optional, boolean flag) - if the timeDelta columns should be added; for this option the value in columns.sort column has to be a time and date that can be parsed with UNIX_TIMESTAMP MySQL function.

![Configuration screenshot](https://github.com/keboola/php-custom-application-hierarchy/blob/master/doc/screenshot.png)

Sample input 
-------------------

id|date|idSourceTweet
---|---|---
A|2012-01-01 12:00:00|NULL
B|2012-01-02 12:00:00|A
C|2012-01-03 12:00:00|B
D|2012-01-04 12:00:00|B
E|2012-01-03 13:00:00|A
F|2012-01-04 12:00:00|A
G|2012-01-04 13:00:00|F
H|2012-01-04 14:00:00|G
I|2012-01-02 13:00:00|NULL
J|2012-01-02 14:00:00|I

The above table represents the following tree

```
A    
|-B  
  |-C
  |-D
|-E  
|-F  
  |-G
    |-H 
I
|-J
```


Sample configuration parameters
-------------------

```
{ 
    "source": "in.c-main.tweets", 
    "destination": "out.c-main.conversations", 
    "columns": {
        "id": "id",
        "sort": "date",
        "parent": "idSourceTweet"
    },
    "timeDelta": 1
}
```

Sample output
-------------------

id|root|position|position_relative|depth|time_delta|time_delta_runsum
---|---|---|---|---|---|---
A|A|0|0|0|0|0
B|A|1|1|1|24|24
C|A|2|2|2|24|48
D|A|3|3|2|24|72
E|A|4|4|1|25|49
F|A|5|5|1|23|72
G|A|6|6|2|1|73
H|A|7|7|3|1|74
I|I|8|0|0|0|0
J|I|9|1|1|1|1

### Result tree representation

```
[position, position_relative, depth] [time_delta, time_delta_runsum]    (time) 

A       [0,0,0] [0,0]   (2012-01-01 12:00:00) 
|-B     [1,1,1] [24,24] (2012-01-02 12:00:00) 
  |-C   [2,2,2] [24,48] (2012-01-03 12:00:00) 
  |-D   [3,3,2] [24,72] (2012-01-04 12:00:00) 
|-E     [4,4,1] [25,49] (2012-01-03 13:00:00) 
|-F     [5,5,1] [23,72] (2012-01-04 12:00:00) 
  |-G   [6,6,2] [1,73]  (2012-01-04 13:00:00) 
    |-H [7,7,3] [1,74]  (2012-01-04 14:00:00) 
I       [8,0,0] [0,0]   (2012-01-02 13:00:00) 
|-J     [9,1,1] [1,1]   (2012-01-02 14:00:00) 
```
