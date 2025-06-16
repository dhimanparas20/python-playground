lst = ["a","c","a","b","z","a","b","x","x","x"]

for index, value in enumerate(lst):
    print(index, value)

def single_occurrences(lst:list[str]):
    return [element for element in lst if lst.count(element) == 1]


def single_occurrences2(lst: list[str]):
    empty_dict = dict()
    for i in lst:
        if i in empty_dict:
            empty_dict[i] += 1
        else:
            empty_dict[i] = 1
    return empty_dict        
    return [key for key, value in empty_dict.items() if value == 1]



# print(single_occurrences(lst))
print(single_occurrences2(lst))
