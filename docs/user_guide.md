

To make create columns starting with constant values those need to be expanded to static  columns using .asCol.
```
df.mutate("user_id", {  "id".asCol(it) + rowNumber() })

```
This is because operator invocation just works left to right and we don't want to change + in a global manner


## Known differences to `dplyr` package in R

* `rename()` will preserve column positions whereas `dplyr::rename` add renamed columns to the end of the table
* The mapping order is inverted in `rename()`. Instead of
   ```
   dplyr::rename(data, new_name=old_name)
   ```
   the kplyr syntax is inverted to be more readible
   ```
   data.rename("old_name" to "new_name")
   ```
* `arrange()` will sort by grouping attributes first, and then per group with the provided sorting attributes.
* `select()` does not silently ignore multiple selections of the same column, but throws an error instead
* `select()` will throw an error if a grouping column is being removed (see [dplyr ticket](https://github.com/hadley/dplyr/issues/1869))
