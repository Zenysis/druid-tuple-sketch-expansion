## Druid Tuple Sketch Expansion
The Tuple Sketch Expansion extension builds on top of the existing [Tuple Sketch](https://druid.apache.org/docs/latest/development/extensions-core/datasketches-tuple.html) module and adds useful new primitives. There is one new aggregator (`arrayOfFilteredDoublesSketch`) and two new post-aggregators (`arrayOfDoublesFilterExpression` and `arrayOfDoublesCollapseExpression`).

### Installation
You can either use the prebuilt jar located in `dist/` or build from source
using the directions below.

Once you have the compiled `jar`, copy it to your druid installation and
follow the
[including extension](https://druid.apache.org/docs/latest/development/extensions.html)
druid documentation. You should end up adding a line similar to this to your
`common.runtime.properties` file:

`druid.extensions.loadList=["druid-tuple-sketch-expansion"]`

**Note:** You will also need to include the druid-datasketches extension `jar` in the same directory as the compiled `jar` since the Tuple Sketch Expansion extension shares code with this core extension. Unfortunately, loading the `druid-datasketches` extension first when Druid starts will not make it so the code is shared. You can find the appropriate druid-datasketches extension `jar` in the public Apache artifact repository. For example, version 0.18.1 can be found [here](https://repository.apache.org/content/groups/public/org/apache/druid/extensions/druid-datasketches/0.18.1/).

##### Building from source
Clone the druid repo and this line to `pom.xml` in the "Community extensions" section:

```xml
<module>${druid-tuple-sketch-expansion}/tuple-sketch-expansion</module>
```
replacing `${druid-tuple-sketch-expansion}` with your path to this repo.

Then, inside the Druid repo, run:
`mvn package -rf :druid-tuple-sketch-expansion`
This will build the extension and place it in
`${druid-tuple-sketch-expansion}/tuple-sketch-expansion/target`


### Aggregators
The `arrayOfFilteredDoublesSketch` is very similar to the native `arrayOfDoublesSketch` provided by the druid-datasketches extension: an array of double values will be associated with each unique key held in the Tuple sketch. The main change over the original aggregation is that you can now pass a query filter alongside a metric column that will determine whether a row's metric value should be *aggregated* with the existing Tuple value for a unique key.
```javascript
{
  "type": "arrayOfFilteredDoublesSketch",
  "fieldName": <metric_name>,
  "metricFilters": <array of MetricFilterSpec>,
  "name": <output_name>,
  "nominalEntries": <number>,
  "numberOfValues": <number>
}
```
|Property|Description|Required?|
|--|--|--|
|type|This String should always be `arrayOfFilteredDoublesSketch`|yes|
|fieldName|The column name who's values are the unique values held in the sketch|yes|
|metricFilters|An array of metric columns and an associated filter object that is used to determine if the metric value for a particular row should be included in the total metric value held in the tuple|yes|
|name|A String for the output (result) name of the calculation|yes|
|nominalEntries|Parameter that determines the accuracy and size of the sketch. Higher k means higher accuracy but more space to store sketches. Must be a power of 2. See the [Theta sketch accuracy](https://datasketches.github.io/docs/Theta/ThetaErrorTable.html) for details.|no, defaults to 16384|
|numberOfValues|The size of the array associated with each distinct key|If not supplied, defaults to the number of `metricFilters`|

The `MetricFilterSpec` structure is:
```javascript
{
  "filter": <Query Filter>,
  "metricColumn": <string>
}
```
Both fields are required. The `filter` field can contain any Druid [query filter](https://druid.apache.org/docs/latest/querying/filters.html).  The `metricColumn` is the string name of the numeric column to collect values for. If you do not need a filter for some of the `metricFilters`, you can supply the `{ "type" : "true" }` native filter that always evaluates to `true`.

On its own, the `arrayOfFilteredDoublesSketch` does not provide much over the native `arrayOfDoublesSketch` aggregation. However, it's a very powerful primitive to have when you combine it with the `arrayOfDoublesFilterExpression` post-aggregator.

### Post Aggregators
##### Remove distinct keys from a Tuple sketch that do not pass an expression filter
Returns a new Tuple sketch containing only entries that can pass the supplied expression filter.
```javascript
{
  "type": "arrayOfDoublesFilterExpression",
  "field": <post-aggregator that refers to a DoublesSketch>,
  "expression": <string>,
  "nominalEntries": <number>
}
```
|Property|Description|Required?|
|--|--|--|
|type|This String should always be `arrayOfDoublesFilterExpression`|yes|
|field|Post Aggregator that refers to a DoublesSketch. It can either be a `fieldAccess` post aggregator or any other type that produces a DoublesSketch.|yes|
|expression|A Druid [expression](https://druid.apache.org/docs/latest/misc/math-expr.html) that either evaluates to a boolean or can be coerced into one. Will be called for each array of doubles in the Tuple sketch.|yes|
|nominalEntries|Parameter that determines the accuracy and size of the sketch|no, defaults to 16384|

The `expression` will only operate on a single array of doubles at a time. Since Druid's expression support is limited, we do not pass the entire array into the expression evaluator. Instead, right before expression evaluation, each value in the array will be stored under the variable `$idx`. So if you have the array `[100, 250, 231]`, the variables available to your expression are `$0`, `$1`, and `$2` where `$0 = 100`, `$1 = 250`, and `$2 = 231`.

When the post aggregator is evaluated, each entry of the incoming Tuple sketch will be looped over. The `expression` will be run for the entry's array of doubles. If the result of the expression is `true`, or is coercible to `true`, the sketch entry will be copied to the output sketch. Otherwise, it will be omitted.

##### Collapse the Tuple sketch values into a single value
Returns a new Tuple sketch where the tuple values for each distinct key have been collapsed into a length-1 tuple containing a single value.
```javascript
{
  "type": "arrayOfDoublesCollapseExpression",
  "field": <post-aggregator that refers to a DoublesSketch>,
  "expression": <string>,
  "nominalEntries": <number>
}
```
|Property|Description|Required?|
|--|--|--|
|type|This String should always be `arrayOfDoublesFilterExpression`|yes|
|field|Post Aggregator that refers to a DoublesSketch. It can either be a `fieldAccess` post aggregator or any other type that produces a DoublesSketch.|yes|
|expression|A Druid [expression](https://druid.apache.org/docs/latest/misc/math-expr.html) that will be evaluated for each tuple in the sketch. The expression should evaluate to a single numeric value. This single value will be used as the new tuple value in the updated sketch. See `arrayOfDoublesFilterExpression` for details about how to format the expression.|yes|
|nominalEntries|Parameter that determines the accuracy and size of the sketch|no, defaults to 16384|

### Example
##### Number of users with 4 or more impressions per week and at least 1 purchase
One really great use of the sketch family is in "cohort" building. This type of analysis tracks a group of unique ids (like `user_id`) for a certain set of properties. In this scenario, we want to find the number of unique users that interact with a theoretical website 4 or more times in a week AND make a purchase.

A hypothetical datasource might be structured with an `event` string dimension that has different properties (like `impression` and `purchase`). The metric `eventCount` stores the number of occurrences of that specific event.

```javascript
{
  ...
  "granularity": "week",
  "aggregations": [
    {
      "type": "arrayOfFilteredDoublesSketch",
      "fieldName": "user_id",
      "name": "user_event_sketch",
      "metricFilters": [
        { "type": "selector", "dimension": "event", "value": "impression" },
        { "type": "selector", "dimension": "event", "value": "purchase" }
      ],
      "nominalEntries": 16384,
      "numberOfValues": 2
    }
  ],
  "postAggregations": [
    {
      "type": "arrayOfDoublesSketchToEstimate",
      "name": "unique_user_count",
      "field": {
        "type": "arrayOfDoublesFilterExpression",
        "field": "user_event_sketch",
        "expression": "$0 >= 4 && $1 > 0",
        "nominalEntries": 16384
      }
    }
  ]
}
````
