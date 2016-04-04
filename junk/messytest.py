from messytables import CSVTableSet, type_guess, \
  types_processor, headers_guess, headers_processor, \
  offset_processor, any_tableset

fh = open('salaries_w_names.csv', 'rb')

# Load a file object:
table_set = CSVTableSet(fh)

# If you aren't sure what kind of file it is, you can use
# any_tableset.
#table_set = any_tableset(fh)

# A table set is a collection of tables:
row_set = table_set.tables[0]

# A row set is an iterator over the table, but it can only
# be run once. To peek, a sample is provided:
print row_set.sample.next()

# guess header names and the offset of the header:
offset, headers = headers_guess(row_set.sample)
row_set.register_processor(headers_processor(headers))

# add one to begin with content, not the header:
row_set.register_processor(offset_processor(offset + 1))

# guess column types:
types = type_guess(row_set.sample, strict=True)

print types

# and tell the row set to apply these types to
# each row when traversing the iterator:
row_set.register_processor(types_processor(types))

# now run some operation on the data:
#for row in row_set:
#  do_something(row)
