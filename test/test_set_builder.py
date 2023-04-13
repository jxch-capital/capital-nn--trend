from builder.set_builder import SetBuilder

set_builder = SetBuilder('../res/slice/')

set_builder.build()

arr = set_builder.random_data_arr(size=100)

print(arr)
