# Aggregate Table Matching

## What Are Aggregate Tables?

In OLAP, aggregate tables are pre-computed summary tables storing results of common
aggregations over a fact table. Instead of scanning millions of rows at query time,
the engine reads a smaller aggregate table with pre-computed sums, counts, or averages
at a coarser granularity.

Example: Given a `sales_fact` table with daily per-product-per-store rows, an aggregate
table `agg_month_sales` might store monthly totals per category.

## Architecture

```
                     AggTableManager
                    (orchestrator)
                   /               \
          PatternbasedRules          ExplicitRules
       (pattern-based)       (schema-defined)
              |                      |
      PatternbasedRecognizer      ExplicitRecognizer
              \                    /
               Recognizer (abstract)
                   |
                AggStar
          (matched aggregate)
```

The engine uses two strategies to discover aggregate tables:

### Pattern-based vs Explicit: Key Differences

| | **Pattern-based** (`PatternbasedRules`) | **Explicit** (`ExplicitRules`) |
|---|---|---|
| **How tables are identified** | Automatically, by naming convention (e.g. `agg_xyz_<factTable>`) | Manually declared in the OLAP schema/cube mapping |
| **How columns are mapped** | Regex templates match column names to measures, levels, foreign keys | Schema author specifies exact column-to-role mappings |
| **Configuration source** | `AggregationMatchRulesSupplier` (OSGi service) | Cube definition in the catalog schema |
| **Recognizer** | `PatternbasedRecognizer` | `ExplicitRecognizer` |
| **Priority** | Fallback -- only tried if no explicit match exists | First -- always checked before pattern-based |
| **Use case** | Convention-over-configuration: works out of the box if aggregate tables follow naming patterns | Full control: needed when table/column names don't follow patterns, or when columns need custom aggregators, fact count overrides, etc. |
| **Zero-config** | Yes, if `instance.basic` or `emf` provides rules at runtime | No, requires schema author to declare each aggregate table |

## Module Structure

```
core/                        API interfaces + runtime matching logic (zero aggmatch deps)
  api/aggmatch/              Plain Java interfaces, enum, supplier
  common/aggmatcher/         AggMatchService, PatternbasedRules, Recognizer, AggTableManager

aggmatch/                    Parent POM (this module)
  record/                    Immutable record implementations of the API interfaces
  instance.basic/            Basic rules supplier (OSGi @Component)
  emf/                       EMF bridge: loads rules from XMI via Ecore model
    model/                   rolap.aggmatch.ecore
```

`core` has zero aggmatch dependencies. It defines the API interfaces and the matching
logic. Both `record` and `emf` depend on `core` for the interfaces.

`core` runs **without** any aggmatch modules -- in that case, no default aggregate
matching happens. If `instance.basic` or `emf` is present at runtime and provides an
`AggregationMatchRulesSupplier`, aggregate matching is enabled.

## Plain Java API (`core/.../api/aggmatch/`)

### Interface Hierarchy

```
AggregationCharacterCase         (enum: IGNORE, EXACT, UPPER, LOWER)

AggregationMatchNameMatcher      (id, charCase?, prefixRegex?, suffixRegex?, nameExtractRegex?)
  +-- AggregationTableMatch      Matches aggregate table names from fact table name
  +-- AggregationFactCountMatch  Matches fact count columns (+ factCountName?)
  +-- AggregationForeignKeyMatch Matches foreign key columns

AggregationMatchRegex            (id, charCase?, template, space?, dot?)

AggregationMatchRegexMapper      (id, regexes list)
  +-- AggregationLevelMap        Matches level columns
  +-- AggregationMeasureMap      Matches measure columns
  +-- AggregationIgnoreMap       Matches columns to ignore

AggregationMatchRule             (tag, enabled?, tableMatch, factCountMatch, ...)
AggregationMatchRules            (aggregationRules list)
AggregationMatchRulesSupplier    extends Supplier<AggregationMatchRules>
```

Fields marked with `?` return `Optional<T>`.

### Key API Methods

| Interface | Method | Return Type | Description |
|-----------|--------|-------------|-------------|
| `AggregationMatchNameMatcher` | `getId()` | `String` | Matcher identifier |
| | `getCharCase()` | `Optional<CharCase>` | Case handling (default: IGNORE) |
| | `getPrefixRegex()` | `Optional<String>` | Regex prepended to the name |
| | `getSuffixRegex()` | `Optional<String>` | Regex appended to the name |
| | `getNameExtractRegex()` | `Optional<String>` | Regex to extract base name via group(1) |
| `AggregationFactCountMatch` | `getFactCountName()` | `Optional<String>` | Custom column name (default: "fact_count") |
| `AggregationMatchRegex` | `getTemplate()` | `String` | Template with `${var}` placeholders (required) |
| | `getSpace()` | `Optional<String>` | Space replacement (default: "_") |
| | `getDot()` | `Optional<String>` | Dot replacement (default: "_") |
| `AggregationMatchRule` | `getTag()` | `String` | Rule identifier (required) |
| | `getEnabled()` | `Optional<Boolean>` | Whether rule is active (default: true) |
| | `getIgnoreMap()` | `Optional<IgnoreMap>` | Optional ignore column matcher |

## Record Implementations (`aggmatch/record/`)

All interfaces have immutable record implementations (e.g. `AggregationTableMatchRecord`,
`AggregationMatchRegexRecord`). Design choices:

- **Fail-fast validation**: Required fields use `Objects.requireNonNull` in compact
  constructors (`AggregationMatchRuleRecord` validates `tag`, `tableMatch`,
  `factCountMatch`, `foreignKeyMatch`, `levelMap`, `measureMap`;
  `AggregationMatchRegexRecord` validates `template`)
- **Optional wrapping**: Nullable fields return `Optional.ofNullable(value)`
- **Defensive copies**: Records with `List` fields use `List.copyOf()` in compact
  constructors; null lists are normalized to `List.of()`

## Basic Rules Supplier (`aggmatch/instance.basic/`)

`BasicAggMatchRulesSupplier` is an OSGi `@Component` that provides the hardcoded
basic matching rules. Rules are created once at class load time as a static constant.

When registered as an OSGi service, `AggTableManager` picks it up via
`RolapContext.getAggMatchRulesSupplier()` and wraps the rules in a `PatternbasedRules`
instance.

### Basic Rule Configuration

**Table match**: `agg_.+_<factTableName>` (case-insensitive)

**Fact count**: column named `fact_count` (case-insensitive)

**Foreign key**: exact column name match (case-insensitive)

**Level map** -- 4 regex patterns (tried in order):
1. `${hierarchy_name}_${level_name}` (LOWER) -- logical name match
2. `${hierarchy_name}_${level_column_name}` (LOWER) -- mixed match
3. `${usage_prefix}${level_column_name}` (EXACT) -- usage prefix match
4. `${level_column_name}` (EXACT) -- physical column match

**Measure map** -- 3 regex patterns:
1. `${measure_name}` (LOWER) -- logical name
2. `${measure_column_name}` (EXACT) -- physical column
3. `${measure_column_name}_${aggregate_name}` (EXACT) -- column + aggregation

## Matching Logic (`core/.../aggmatcher/AggMatchService.java`)

`AggMatchService` is a static utility that compiles the API interfaces into
`Recognizer.Matcher` lambdas (`boolean matches(String name)`).

### Two Matcher Families

**NameMatcher** -- for table names, fact count columns, foreign keys:
```
Pattern = prefixRegex + name + suffixRegex
```
Character case handling: IGNORE (case-insensitive), EXACT, UPPER, LOWER.
Optional `nameExtractRegex` extracts a capture group from the input name before
building the pattern.

**RegexMapper** -- for levels, measures, ignored columns:
```
Template = "${hierarchy_name}_${level_name}"  ->  "time_year"
```
Multiple regex patterns are OR-ed. Template variables (`${var}`) are substituted
with actual metadata names. Space and dot characters in names are replaced
(default: `_`).

### Public Methods

| Method | Input | Purpose |
|--------|-------|---------|
| `createTableMatcher` | TableMatch + factTableName | Match aggregate table names |
| `createFactCountMatcher` | FactCountMatch | Match fact count columns |
| `createForeignKeyMatcher` | ForeignKeyMatch + fkName | Match foreign key columns |
| `createLevelMatcher` | LevelMap + hierarchy/level/column names | Match level columns |
| `createMeasureMatcher` | MeasureMap + measure/column/agg names | Match measure columns |
| `createIgnoreMatcher` | IgnoreMap | Match ignored columns |

### PatternbasedRules Optional Handling

`PatternbasedRules` uses the Optional API for clean rule resolution:

- **Null-safe rule lookup**: `requireAggRule()` throws `IllegalStateException` if
  no enabled rule matches the tag
- **Enabled check**: `rule.getEnabled().orElse(true)` -- rules without an explicit
  `enabled` flag default to active
- **Ignore matcher**: `rule.getIgnoreMap().map(...).orElse(name -> false)` -- if no
  ignore map is configured, no columns are ignored

## Matching Flow (AggTableManager)

The two strategies are tried in order -- explicit first, pattern-based second:

```
1. Get optional AggregationMatchRulesSupplier from context
2. Load JdbcSchema (database metadata)
3. For each RolapStar:
   a. bindToStar() -- annotate fact table columns as MEASURE or FOREIGN_KEY
   b. For each candidate database table:
      i.   Skip if excluded by ExplicitRules
      ii.  Try ExplicitRules match first (schema-declared aggregate)
             -> ExplicitRecognizer validates columns
      iii. If no explicit match AND PatternbasedRules are available:
             -> PatternbasedRules.matchesTableName() checks naming convention
             -> PatternbasedRecognizer validates columns
      iv.  If either strategy validates: create AggStar
```

Both recognizers extend the abstract `Recognizer` and run the same seven-step
column validation (see below), but they differ in **how** they obtain matchers:
`PatternbasedRecognizer` builds matchers from regex templates via `AggMatchService`,
while `ExplicitRecognizer` uses the exact column mappings declared in the schema.

### Recognizer.check() -- Column Validation

Seven sequential steps:

1. **checkIgnores()** -- mark columns matching IgnoreMap as IGNORE
2. **checkFactColumns()** -- find the fact count column (exactly one required)
3. **checkMeasures()** -- match measure columns via MeasureMap
4. **checkLevels()** -- match level columns via LevelMap
5. **checkForeignKeys()** -- match foreign key columns
6. **checkUnused()** -- warn about unrecognized columns
7. **checkLost()** -- verify all fact table elements have corresponding aggregate columns

## EMF Bridge (`aggmatch/emf/`)

For loading custom rules from XMI files.

### Ecore Model (`emf/model/rolap.aggmatch.ecore`)

Namespace URI: `https://www.daanse.org/spec/org.eclipse.daanse.rolap.aggmatch`

```
AggregationMatchBase (abstract)
  +-- AggregationMatchCaseMatcher (abstract)
  |     +-- AggregationMatchNameMatcher (abstract)
  |     |     +-- AggregationTableMatch
  |     |     +-- AggregationFactCountMatch
  |     |     +-- AggregationForeignKeyMatch
  |     +-- AggregationMatchRegex
  |
  +-- AggregationMatchRegexMapper (abstract)
  |     +-- AggregationLevelMap
  |     +-- AggregationMeasureMap
  |     +-- AggregationIgnoreMap
  |
  +-- AggregationMatchRef (abstract)
        +-- AggregationTableMatchRef
        +-- AggregationFactCountMatchRef
        +-- AggregationForeignKeyMatchRef
        +-- AggregationLevelMapRef
        +-- AggregationMeasureMapRef
        +-- AggregationIgnoreMapRef

AggregationMatchRule (tag, inline or ref for each matcher)
AggregationMatchRules (top-level container, shared matchers + rules list)
```

**EMF attribute name mapping** -- EMF attributes differ from the Java API:

| EMF Ecore attribute | Java API method | Description |
|---------------------|-----------------|-------------|
| `pretemplate` | `getPrefixRegex()` | Regex prepended to the name |
| `posttemplate` | `getSuffixRegex()` | Regex appended to the name |
| `basename` | `getNameExtractRegex()` | Regex to extract base name via group(1) |

Ref types allow shared matchers defined at the `AggregationMatchRules` level to be
referenced by `refId` from multiple rules.

### XMI Example

The following XMI file reproduces the basic rules (equivalent to
`BasicAggMatchRulesSupplier`):

```xml
<?xml version="1.0" encoding="UTF-8"?>
<aggm:AggregationMatchRules
    xmi:version="2.0"
    xmlns:xmi="http://www.omg.org/XMI"
    xmlns:aggm="https://www.daanse.org/spec/org.eclipse.daanse.rolap.aggmatch"
    tag="basic-rules">

  <aggregationRules tag="default">

    <tableMatch id="ta" charCase="IGNORE" pretemplate="agg_.+_"/>

    <factCountMatch id="fca" charCase="IGNORE"/>

    <foreignKeyMatch id="fka" charCase="IGNORE"/>

    <levelMap id="lxx">
      <regexes id="logical"  charCase="LOWER" template="${hierarchy_name}_${level_name}"/>
      <regexes id="mixed"    charCase="LOWER" template="${hierarchy_name}_${level_column_name}"/>
      <regexes id="usage"    charCase="EXACT" template="${usage_prefix}${level_column_name}"/>
      <regexes id="physical" charCase="EXACT" template="${level_column_name}"/>
    </levelMap>

    <measureMap id="mxx">
      <regexes id="logical"    charCase="LOWER" template="${measure_name}"/>
      <regexes id="foreignkey" charCase="EXACT" template="${measure_column_name}"/>
      <regexes id="physical"   charCase="EXACT" template="${measure_column_name}_${aggregate_name}"/>
    </measureMap>

  </aggregationRules>
</aggm:AggregationMatchRules>
```

### XMI with Shared Matchers and Refs

When multiple rules reuse the same matchers:

```xml
<?xml version="1.0" encoding="UTF-8"?>
<aggm:AggregationMatchRules
    xmi:version="2.0"
    xmlns:xmi="http://www.omg.org/XMI"
    xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
    xmlns:aggm="https://www.daanse.org/spec/org.eclipse.daanse.rolap.aggmatch"
    tag="multi-rule-config">

  <!-- Shared matchers (referenced by id from rules) -->
  <tableMatches id="shared-table" charCase="IGNORE" pretemplate="agg_.+_"/>
  <factCountMatches id="shared-fc" charCase="IGNORE"/>
  <foreignKeyMatches id="shared-fk" charCase="IGNORE"/>
  <levelMaps id="shared-levels">
    <regexes id="l1" charCase="LOWER" template="${hierarchy_name}_${level_name}"/>
    <regexes id="l2" charCase="EXACT" template="${level_column_name}"/>
  </levelMaps>

  <!-- Rule "daily" uses shared matchers via refs -->
  <aggregationRules tag="daily">
    <tableMatchRef refId="shared-table"/>
    <factCountMatchRef refId="shared-fc"/>
    <foreignKeyMatchRef refId="shared-fk"/>
    <levelMapRef refId="shared-levels"/>
    <measureMap id="daily-measures">
      <regexes id="m1" charCase="LOWER" template="${measure_name}"/>
    </measureMap>
  </aggregationRules>

  <!-- Rule "monthly" uses shared matchers via refs, with different measures -->
  <aggregationRules tag="monthly">
    <tableMatchRef refId="shared-table"/>
    <factCountMatchRef refId="shared-fc"/>
    <foreignKeyMatchRef refId="shared-fk"/>
    <levelMapRef refId="shared-levels"/>
    <measureMap id="monthly-measures">
      <regexes id="m1" charCase="EXACT" template="${measure_column_name}_${aggregate_name}"/>
    </measureMap>
  </aggregationRules>
</aggm:AggregationMatchRules>
```

### OSGi Configuration

To load an XMI file at runtime, configure `EmfAggMatchProvider` via Configuration Admin:

```
service.pid = org.eclipse.daanse.rolap.aggmatch.emf.EmfAggMatchProvider
resource_url = /path/to/aggmatch-rules.xmi
```

### Components

**EmfAggMatchProvider** -- OSGi `@Component` implementing `AggregationMatchRulesSupplier`:
- Injects EMF `ResourceSet` via `@Reference(target = "(nsuri=...)")`
- Loads XMI file configured via `EmfAggMatchProviderConfig.resource_url()`
- Validates `resource_url` is not null or blank on activation
- Converts EMF objects to plain Java records via `EmfAggMatchConverter`
- Thread-safe: `rules` field is `volatile`

**EmfAggMatchConverter** -- static converter:
- Maps each EMF type to its record counterpart
- Converts `AggregationCharacterCase` enum values by name
- Null-safe: `convertRegexes()` returns `List.of()` for null input

## Aggregate Table Structure

### Example Schema

Given a fact table `sales_fact`:

| Column | Type | Role |
|--------|------|------|
| `product_id` | INTEGER | Foreign key to `product` dimension |
| `store_id` | INTEGER | Foreign key to `store` dimension |
| `time_id` | INTEGER | Foreign key to `time` dimension |
| `unit_sales` | DECIMAL | Measure |
| `store_sales` | DECIMAL | Measure |
| `store_cost` | DECIMAL | Measure |

### Aggregate Table: Monthly by Product Category

Table name: `agg_monthly_sales_fact` (matches `agg_.+_sales_fact`)

| Column | Type | Matched as | Match rule |
|--------|------|------------|------------|
| `fact_count` | INTEGER | Fact count | FactCountMatch: column name = "fact_count" |
| `product_id` | INTEGER | Foreign key | ForeignKeyMatch: same name as fact table FK |
| `time_month` | VARCHAR | Level | LevelMap: `${hierarchy_name}_${level_name}` -> "time_month" |
| `unit_sales` | DECIMAL | Measure | MeasureMap: `${measure_name}` -> "unit_sales" |
| `store_sales` | DECIMAL | Measure | MeasureMap: `${measure_name}` -> "store_sales" |
| `store_cost` | DECIMAL | Measure | MeasureMap: `${measure_name}` -> "store_cost" |

### Aggregate Table: Yearly Totals

Table name: `agg_yearly_sales_fact`

| Column | Type | Matched as | Match rule |
|--------|------|------------|------------|
| `fact_count` | INTEGER | Fact count | FactCountMatch |
| `time_year` | INTEGER | Level | LevelMap: `${hierarchy_name}_${level_name}` -> "time_year" |
| `unit_sales_sum` | DECIMAL | Measure | MeasureMap: `${measure_column_name}_${aggregate_name}` |
| `store_sales_sum` | DECIMAL | Measure | MeasureMap: `${measure_column_name}_${aggregate_name}` |

### Column Role Summary

Every column in an aggregate table must be recognized as one of:

| Role | Required | Description |
|------|----------|-------------|
| **Fact count** | Exactly 1 | Number of fact rows aggregated into this row |
| **Measure** | 0..* | Aggregated values (sum, count, avg, etc.) |
| **Level** | 0..* | Dimension level at which data is aggregated |
| **Foreign key** | 0..* | Reference to a dimension table |
| **Ignore** | 0..* | Columns excluded from matching (via IgnoreMap) |

Unrecognized columns cause a warning. If any fact table measure or foreign key
has no corresponding column in the aggregate table, the aggregate is rejected.

## Tests

**Core tests** (30 tests in `core/src/test/java/.../aggmatcher/`):

- **PatternbasedRuleTest** (14 tests) -- tests three rule configurations ("default", "bbbb",
  "cccc") covering table name matching, fact count matching, foreign key matching with
  nameExtractRegex and case handling, level matching with usage prefixes and dot/space
  replacement, and measure matching.

- **AggMatchServiceTest** (16 tests) -- usage prefix level matching, aggregate table
  discovery patterns, distinct-count measures, dotted hierarchy names, fact count column
  variations, and foreign key matching.

**EMF tests** (21 tests in `aggmatch/emf/src/test/java/.../emf/`):

- **EmfAggMatchConverterTest** (15 tests) -- conversion of each EMF type to its record
  counterpart, all `AggregationCharacterCase` enum values, custom space/dot replacements,
  and comparison of EMF-converted rules against `BasicAggMatchRulesSupplier` output.

- **EmfAggMatchXmiRoundtripTest** (6 tests) -- generates XMI files programmatically via
  EMF, then loads and converts them, verifying minimal, full, and basic rule configurations.


The documentation was partially generated with ai.
