word = 'hello'

# word.remove(word[:1])

print(word[-4:])

print(word[:-1])


states = [25, 4]


predictions = []

dict_1 = {25:[1,2,3], 4:[3,2,19]}

for i in range(len(dict_1[25])):
    num = 0
    for s in states:
        num += dict_1[s][i]
    predictions.append(num/len(states))
    num = 0

for p in predictions:
    print(p)