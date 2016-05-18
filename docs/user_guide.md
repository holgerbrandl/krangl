

To make create columns starting with constant values those need to be expanded to static  columns using .asCol.
```
df.mutate("user_id", {  "id".asCol(it) + rowNumber() })

```
This is because operator invocation just works left to right and we don't want to change + in a global manner
