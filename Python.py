# Databricks notebook source
print(3 + 3)
print(2 * 5)
print("I am", 35, "years old.")
print(True+1)
print("Hello", "World")
print("Hello", "World", sep="-")
print("Hello", end=" ")
print("World")
print("Age:", 25)

# COMMAND ----------

name = "Asha"
age = 25
print(f"My name is {name} and age is {age}")

# COMMAND ----------

print("Hello\nWorld")

# COMMAND ----------

print("A\tB\tC")

# COMMAND ----------

print(10 > 5)

# COMMAND ----------

print(10 + 5 * 2)

# COMMAND ----------

a = [1, 2, 3]
print(a)

# COMMAND ----------

a = [1, 2, 3]
print(*a)

# COMMAND ----------

d = {"a": 1, "b": 2}
print(d)

# COMMAND ----------

print("Name: {}, Age: {}".format("Asha", 25))  # Inserts "Asha" and 25 into the curly braces using .format()

# COMMAND ----------

a = None
print(a)

# COMMAND ----------

def test():
    print("Hello")
    
print(test())

#👉 print() shows something on screen
#👉 return sends something back from a function
#If there is no return, Python automatically returns None.

#print(test())-->Test()-->o/p-Hello. Now it will print the output of test() but since there is no function inside retun, it will print None

# COMMAND ----------

print("Age:", 25)           # Uses comma to separate values; automatically adds a space and converts 25 to string
print("Age:" + str(25))     # Concatenates strings; explicitly converts 25 to string before joining

# COMMAND ----------

a = 10
print(id(a))

#The variable a is assigned the value 10. The function id(a) returns the unique identifier (memory address) of the object that a refers to. 


# COMMAND ----------

x = 5
y = "John"
print(x)
print(y)

# COMMAND ----------

x = 4       # x is of type int
x = "Sally" # x is now of type str
print(x)

# COMMAND ----------

x = str(3)    # x will be '3'
y = int(3)    # y will be 3
z = float(3)  # z 
print(x)
print(y)
print(z)

# COMMAND ----------

x = 5
y = "John"
print(type(x))
print(type(y))

# COMMAND ----------

x, y, z = "Orange", "Banana", "Cherry"
print(x)
print(y)
print(z)

# COMMAND ----------

x = y = z = "Orange"
print(x)
print(y)
print(z)

# COMMAND ----------

fruits = ["apple", "banana", "cherry"]  # List with three fruit names
x, y, z = fruits                        # Unpacks the list into three variables: x='apple', y='banana', z='cherry'
print(x)                                # Prints 'apple'
print(y)                                # Prints 'banana'
print(z)                                # Prints 'cherry'

# COMMAND ----------

x = "Python is awesome"
print(x)

# COMMAND ----------

x = "Python"
y = "is"
z = "awesome"
print(x, y, z)

# COMMAND ----------

x = "Python "
y = "is "
z = "awesome"
print(x + y + z)

# COMMAND ----------

x = 5
y = 10
print(x + y)

# COMMAND ----------

# MAGIC %skip
# MAGIC x = 5
# MAGIC y = "John"
# MAGIC print(x + y)
# MAGIC
# MAGIC It will give an error

# COMMAND ----------

x = 5
y = "John"
print(x, y)

# COMMAND ----------

x = "awesome"

def myfunc():
  print("Python is " + x)

myfunc()

# COMMAND ----------

x = "awesome"  # Global variable x

def myfunc():
  x = "fantastic"  # Local variable x inside the function
  print("Python is " + x)  # Uses local x

myfunc()  # Prints "Python is fantastic"

print("Python is " + x)  # Uses global x, prints "Python is awesome"

# COMMAND ----------

def myfunc():
  global x  # Declares x as a global variable, so assignments affect the global scope
  x = "fantastic"  # Sets the global variable x to "fantastic"

myfunc()  # Calls the function, updating x globally

print("Python is " + x)  # Prints "Python is fantastic" using the updated global x

#normally, when you create a variable inside a function, that variable is local, and can only be used inside that function.To create a global variable inside a function, you can use the global keyword

# COMMAND ----------

x = "awesome"  # Global variable x

def myfunc():
  global x      # Declares x as global, so assignments affect the global scope
  x = "fantastic"  # Sets the global variable x to "fantastic"

myfunc()  # Calls the function, updating x globally

print("Python is " + x)  # Prints "Python is fantastic" using the updated global x

# When you use 'global x' inside a function, any assignment to x updates the global variable, not a local one.
# After calling myfunc(), the global x is changed to "fantastic".

# COMMAND ----------

x='Welcome'           # x is a string: 'Welcome'
print(x[3])           # Strings in Python are sequences, so you can access characters by index like arrays

# COMMAND ----------

# Python has several built-in data types. Here are the main ones with examples:

# Numeric Types
int_var = 10            # Integer: whole numbers
float_var = 10.5        # Float: decimal numbers
complex_var = 2 + 3j    # Complex: numbers with real and imaginary parts

# Sequence Types
str_var = "Hello"       # String: sequence of Unicode characters
list_var = [1, 2, 3]    # List: ordered, mutable collection
tuple_var = (1, 2, 3)   # Tuple: ordered, immutable collection
range_var = range(5)    # Range: sequence of numbers, often used in for-loops

# Mapping Type
dict_var = {"a": 1, "b": 2}  # Dictionary: key-value pairs, mutable

# Set Types
set_var = {1, 2, 3}          # Set: unordered, unique elements
frozenset_var = frozenset([1, 2, 3])  # Frozenset: immutable set

# Boolean Type
bool_var = True              # Boolean: True or False

# Binary Types
bytes_var = b"Hello"         # Bytes: immutable sequence of bytes
bytearray_var = bytearray(5) # Bytearray: mutable sequence of bytes
memoryview_var = memoryview(bytes_var) # Memoryview: memory view object

# None Type
none_var = None              # NoneType: represents absence of value


# COMMAND ----------

x = 5
print(type(x))

# COMMAND ----------

# MAGIC %skip
# MAGIC x = 1    # int
# MAGIC y = 2.8  # float
# MAGIC z = 1j   # complex

# COMMAND ----------

x = 1
y = 35656222554887711
z = -3255522

print(type(x))
print(type(y))
print(type(z))

# COMMAND ----------

x = 1.10
y = 1.0
z = -35.59

print(type(x))
print(type(y))
print(type(z))

# COMMAND ----------

x = 35e3
y = 12E4
z = -87.7e100

print(type(x))
print(type(y))
print(type(z))


# COMMAND ----------

x = 3+5j
y = 5j
z = -5j
print(type(x))
print(type(y))
print(type(z))

# COMMAND ----------

x = 1    # int
y = 2.8  # float
z = 1j   # complex

#convert from int to float:
a = float(x)

#convert from float to int:
b = int(y)

#convert from int to complex:
c = complex(x)

print(a)
print(b)
print(c)

print(type(a))
print(type(b))
print(type(c))

# COMMAND ----------

import random

print(random.randrange(1, 10))


# COMMAND ----------

x = int(1)   # x will be 1
y = int(2.8) # y will be 2
z = int("3") # z will be 3
print (x)
print(y)
print(z)

# COMMAND ----------

x = float(1)     # x will be 1.0
y = float(2.8)   # y will be 2.8
z = float("3")   # z will be 3.0
w = float("4.2") # w will be 4.2
print(x)
print(y)
print(z)
print(w)


# COMMAND ----------

x = str("s1") # x will be 's1'
y = str(2)    # y will be '2'
z = str(3.0)  # z will be '3.0'
print(x)
print(y)
print(z)

# COMMAND ----------

# MAGIC %md
# MAGIC #String

# COMMAND ----------

print("Hello")
print('Hello')
#both are same

# COMMAND ----------

print("It's alright")
print("He is called 'Johnny'")
print('He is called "Johnny"')

# COMMAND ----------

a = "Hello"
print(a)

# COMMAND ----------

#multiline string

a = """Lorem ipsum dolor sit amet,
consectetur adipiscing elit,
sed do eiusmod tempor incididunt
ut labore et dolore magna aliqua."""
print(a)

# COMMAND ----------

b = "Hello, World!"
# Slicing extracts a substring from index 2 up to (but not including) index 5
print(b[2:5])  # Output: 'llo'

# COMMAND ----------

b = "Hello, World!"
print(b[:5])

# COMMAND ----------

#Uppercase
a = "Hello, World!"
print(a.upper())

# COMMAND ----------

#lowecase
a = "Hello, World!"
print(a.lower())

# COMMAND ----------

#The strip() method removes any whitespace from the beginning or the end:
a = " Hello, World! "
print(a.strip()) # returns "Hello, World!"


# COMMAND ----------

#The replace() method replaces a string with another string:

a = "Hello, World!"
print(a.replace("H", "J"))

# COMMAND ----------

#The split() method splits the string into substrings if it finds instances of the separator:

a = "Hello, World!"
print(a.split(",")) # returns ['Hello', ' World!']

# COMMAND ----------

a = "Hello"
b = "World"
c = a + b
print(c)

# COMMAND ----------

a = "Hello"
b = "World"
c = a + " " + b
print(c)

# COMMAND ----------

age = 36
txt = f"My name is John, I am {age}"
print(txt)

# COMMAND ----------

price = 59
txt = f"The price is {price} dollars"
print(txt)

# COMMAND ----------

price = 59
txt = f"The price is {price:.2f} dollars"
print(txt)

# COMMAND ----------

txt = f"The price is {20 * 59} dollars"
print(txt)

# COMMAND ----------

txt = "We are the so-called \"Vikings\" from the north."
print(txt)

# COMMAND ----------

# MAGIC %skip
# MAGIC capitalize()	Converts the first character to upper case
# MAGIC casefold()	Converts string into lower case
# MAGIC center()	Returns a centered string
# MAGIC count()	Returns the number of times a specified value occurs in a string
# MAGIC encode()	Returns an encoded version of the string
# MAGIC endswith()	Returns true if the string ends with the specified value
# MAGIC expandtabs()	Sets the tab size of the string
# MAGIC find()	Searches the string for a specified value and returns the position of where it was found
# MAGIC format()	Formats specified values in a string
# MAGIC format_map()	Formats specified values in a string
# MAGIC index()	Searches the string for a specified value and returns the position of where it was found
# MAGIC isalnum()	Returns True if all characters in the string are alphanumeric
# MAGIC isalpha()	Returns True if all characters in the string are in the alphabet
# MAGIC isascii()	Returns True if all characters in the string are ascii characters
# MAGIC isdecimal()	Returns True if all characters in the string are decimals
# MAGIC isdigit()	Returns True if all characters in the string are digits
# MAGIC isidentifier()	Returns True if the string is an identifier
# MAGIC islower()	Returns True if all characters in the string are lower case
# MAGIC isnumeric()	Returns True if all characters in the string are numeric
# MAGIC isprintable()	Returns True if all characters in the string are printable
# MAGIC isspace()	Returns True if all characters in the string are whitespaces
# MAGIC istitle()	Returns True if the string follows the rules of a title
# MAGIC isupper()	Returns True if all characters in the string are upper case
# MAGIC join()	Joins the elements of an iterable to the end of the string
# MAGIC ljust()	Returns a left justified version of the string
# MAGIC lower()	Converts a string into lower case
# MAGIC lstrip()	Returns a left trim version of the string
# MAGIC maketrans()	Returns a translation table to be used in translations
# MAGIC partition()	Returns a tuple where the string is parted into three parts
# MAGIC replace()	Returns a string where a specified value is replaced with a specified value
# MAGIC rfind()	Searches the string for a specified value and returns the last position of where it was found
# MAGIC rindex()	Searches the string for a specified value and returns the last position of where it was found
# MAGIC rjust()	Returns a right justified version of the string
# MAGIC rpartition()	Returns a tuple where the string is parted into three parts
# MAGIC rsplit()	Splits the string at the specified separator, and returns a list
# MAGIC rstrip()	Returns a right trim version of the string
# MAGIC split()	Splits the string at the specified separator, and returns a list
# MAGIC splitlines()	Splits the string at line breaks and returns a list
# MAGIC startswith()	Returns true if the string starts with the specified value
# MAGIC strip()	Returns a trimmed version of the string
# MAGIC swapcase()	Swaps cases, lower case becomes upper case and vice versa
# MAGIC title()	Converts the first character of each word to upper case
# MAGIC translate()	Returns a translated string
# MAGIC upper()	Converts a string into upper case
# MAGIC zfill()	Fills the string with a specified number of 0 values at the beginning
# MAGIC

# COMMAND ----------

print(10 > 9)
print(10 == 9)
print(10 < 9)

# COMMAND ----------

a = 200
b = 33

if b > a:
  print("b is greater than a")
else:
  print("b is not greater than a")

# COMMAND ----------

# bool() returns True for any non-empty string or non-zero number
print(bool("Hello"))  # True, because "Hello" is a non-empty string
print(bool(15))       # True, because 15 is a non-zero number

# COMMAND ----------

x = "Hello"
y = 15

print(bool(x))
print(bool(y))

# COMMAND ----------

print(10 + 5)


# COMMAND ----------

sum1 = 100 + 50      # 150 (100 + 50)
sum2 = sum1 + 250    # 400 (150 + 250)
sum3 = sum2 + sum2   # 800 (400 + 400)

print(sum1)

# COMMAND ----------

#Arithmetic Operators

x = 15
y = 4

# Addition
print(x + y)    # 19

# Subtraction
print(x - y)    # 11

# Multiplication
print(x * y)    # 60

# Division (float)
print(x / y)    # 3.75

# Modulus (remainder)
print(x % y)    # 3

# Exponentiation
print(x ** y)   # 50625

# Floor division (integer division)
print(x // y)   # 3

# / - Division (returns a float)
# // - Floor division (returns an integer)

# COMMAND ----------

#Assignment Operators

x = 5
x += 3  # Equivalent to x = x + 3
print(x)


x = 5  # 5 in binary: 0b0101

x |= 3  # 3 in binary: 0b0011
# Bitwise OR: 0b0101 | 0b0011 = 0b0111 (which is 7)

print(x)  # Output: 7

# COMMAND ----------

#Comaprison Operator

x = 5
y = 3

print(x == y)
print(x != y)
print(x > y)
print(x < y)
print(x >= y)
print(x <= y)

# COMMAND ----------

#Logical

x = 5

print(1 < x < 10)

print(1 < x and x < 10)

# COMMAND ----------

x = 5

print(x > 0 and x < 10)

# COMMAND ----------

x = 5

print(x < 5 or x > 10)

# COMMAND ----------

x = 5

print(not(x > 3 and x < 10))

# COMMAND ----------

#identity Operator-The is operator returns True if both variables point to the same object:

x = ["apple", "banana"]
y = ["apple", "banana"]
z = x

print(x is z)
print(x is y)
print(x == y)

# COMMAND ----------

#is - Checks if both variables point to the same object in memory
#== - Checks if the values of both variables are equal


x = [1, 2, 3]
y = [1, 2, 3]

print(x == y)
print(x is y)


# COMMAND ----------

x = ["apple", "banana"]
y = ["apple", "banana"]

print(x is not y)

# COMMAND ----------

#Membership Operator

fruits = ["apple", "banana", "cherry"]

print("banana" in fruits)

# COMMAND ----------

fruits = ["apple", "banana", "cherry"]

print("pineapple" not in fruits)

# COMMAND ----------

text = "Hello World"

print("H" in text)
print("Hello" in text)
print("hello" in text)
print("z" not in text)

# COMMAND ----------

print((6 + 3) - (6 + 3))


# COMMAND ----------

print(100 + 5 * 3)


# COMMAND ----------

print(5 + 4 - 7 + 3)


# COMMAND ----------

# A list in Python is an ordered, mutable collection that can hold items of any data type.
# Lists are defined using square brackets [] and can contain duplicate elements.
# You can access, modify, add, or remove elements from a list.

thislist = ["apple", "banana", "cherry"]
print(len(thislist))  # Returns the number of items in the list

# COMMAND ----------

# Different types of lists in Python:
list1 = ["apple", "banana", "cherry"]  # List of strings
list2 = [1, 5, 7, 9, 3]               # List of integers
list3 = [True, False, False]           # List of booleans
list4 = [1, "apple", True, 3.14]       # List with mixed data types

# COMMAND ----------

# This is a list in Python containing mixed data types: a string, an integer, a boolean, another integer, and another string.
list1 = ["abc", 34, True, 40, "male"]

# COMMAND ----------

mylist = ["apple", "banana", "cherry"]
print(type(mylist))

# COMMAND ----------

# The list() constructor can be used to create a list from any iterable, such as a tuple.
# Here, ("apple", "banana", "cherry") is a tuple, and list() converts it to a list.
thislist = list(("apple", "banana", "cherry")) # note the double round-brackets
print(thislist)

# COMMAND ----------

#List literal is used to directly create lists, while list() is used to convert iterables into lists.


# Creating a list using square brackets []
list_a = ["apple", "banana", "cherry"]

# Creating a list using the list() constructor
list_b = list(("apple", "banana", "cherry"))  # Converts tuple to list

print(type(list_a))  # <class 'list'>
print(type(list_b))  # <class 'list'>
print(list_a == list_b)  # True, both lists have the same elements

# Difference:
# list_a is created directly using square brackets []
# list_b is created using the list() constructor, which can convert any iterable (like a tuple) to a list

# COMMAND ----------

#Accessing items within the List
thislist = ["apple", "banana", "cherry"]
print(thislist[1])

# COMMAND ----------

#-1 refers to the last item, -2 refers to the second last item etc.
thislist = ["apple", "banana", "cherry"]
print(thislist[-1])

# COMMAND ----------

thislist = ["apple", "banana", "cherry", "orange", "kiwi", "melon", "mango"]
print(thislist[2:5])

# COMMAND ----------

# This code creates a list called 'thislist' with seven fruit names.
# The slice [:4] returns the first four items in the list (from index 0 up to, but not including, index 4).
thislist = ["apple", "banana", "cherry", "orange", "kiwi", "melon", "mango"]
print(thislist[:4])  # Output: ['apple', 'banana', 'cherry', 'orange']

# COMMAND ----------

# This code creates a list called 'thislist' with several fruit names.
# The slice thislist[2:] returns all elements from index 2 (inclusive) to the end of the list.
# In this case, it will print: ['cherry', 'orange', 'kiwi', 'melon', 'mango']

thislist = ["apple", "banana", "cherry", "orange", "kiwi", "melon", "mango"]
print(thislist[2:])

# COMMAND ----------

# You can change the value of a specific item in a list by assigning a new value to its index.
# Here, we change the item at index 1 ("banana") to "blackcurrant".
thislist = ["apple", "banana", "cherry"]
thislist[1] = "blackcurrant"
print(thislist)  # Output: ['apple', 'blackcurrant', 'cherry']

# COMMAND ----------

# This code replaces a slice of the list (from index 1 to 2, inclusive) with new values.
# The slice thislist[1:3] refers to the items at index 1 ("banana") and index 2 ("cherry").
# These two items are replaced with "blackcurrant" and "watermelon".
# The resulting list will be: ['apple', 'blackcurrant', 'watermelon', 'orange', 'kiwi', 'mango']

thislist = ["apple", "banana", "cherry", "orange", "kiwi", "mango"]
thislist[1:3] = ["blackcurrant", "watermelon"]
print(thislist)

# COMMAND ----------

# This code replaces a slice of the list (from index 1 to 1, inclusive) with new values.
# The slice thislist[1:2] refers to the item at index 1 ("banana").
# This item is replaced with "blackcurrant" and "watermelon", expanding the list.
# The resulting list will be: ['apple', 'blackcurrant', 'watermelon', 'cherry']

thislist = ["apple", "banana", "cherry"]
thislist[1:2] = ["blackcurrant", "watermelon"]
print(thislist)

# COMMAND ----------

# This code replaces a slice of the list (from index 1 to 2, inclusive) with a single new value.
# The slice thislist[1:3] refers to the items at index 1 ("banana") and index 2 ("cherry").
# These two items are replaced with "watermelon", so the resulting list will be: ['apple', 'watermelon']

thislist = ["apple", "banana", "cherry"]
thislist[1:3] = ["watermelon"]
print(thislist)

# COMMAND ----------

# You can add an item to the end of a list using the append() method.
# Here, "orange" is added to the end of the list.
thislist = ["apple", "banana", "cherry"]
thislist.append("orange")
print(thislist)  # Output: ['apple', 'banana', 'cherry', 'orange']

# COMMAND ----------

# The insert() method adds an item at a specified index in the list.
# Here, "orange" is inserted at index 1, shifting the other items to the right.
# The resulting list will be: ['apple', 'orange', 'banana', 'cherry']

thislist = ["apple", "banana", "cherry"]
thislist.insert(1, "orange")
print(thislist)

# COMMAND ----------

#remove list item 

thislist = ["apple", "banana", "cherry"]
thislist.remove("banana")
print(thislist)

# COMMAND ----------

#removes only the first occurance
thislist = ["apple", "banana", "cherry", "banana", "kiwi"]
thislist.remove("banana")
print(thislist)

# COMMAND ----------

#The pop() method removes the specified index.
thislist = ["apple", "banana", "cherry"]
thislist.pop(1)
print(thislist)

# COMMAND ----------

#If you do not specify the index, the pop() method removes the last item.
thislist = ["apple", "banana", "cherry"]
thislist.pop()
print(thislist)



# COMMAND ----------

#The del keyword also removes the specified index:
thislist = ["apple", "banana", "cherry"]
del thislist[0]
print(thislist)

# COMMAND ----------

#The del keyword can also delete the list completely.

thislist = ["apple", "banana", "cherry"]
del thislist

# COMMAND ----------

#Looping through a list 
thislist = ["apple", "banana", "cherry"]
for x in thislist:
  print(x)

# COMMAND ----------

#Loop Through the Index Numbers
thislist = ["apple", "banana", "cherry"]
for i in range(len(thislist)):
  print(thislist[i])

# COMMAND ----------

# This code creates a new list containing only the fruits that have the letter "a" in their name.
fruits = ["apple", "banana", "cherry", "kiwi", "mango"]
newlist = []

for x in fruits:
  if "a" in x:  # Checks if the letter "a" is present in the fruit name
    newlist.append(x)  # Adds the fruit to newlist if condition is true

print(newlist)  # Output: ['apple', 'banana', 'mango']

# COMMAND ----------

#sorting
thislist = ["orange", "mango", "kiwi", "pineapple", "banana"]
thislist.sort()
print(thislist)

# COMMAND ----------

thislist = [100, 50, 65, 82, 23]
thislist.sort()
print(thislist)

# COMMAND ----------

thislist = ["orange", "mango", "kiwi", "pineapple", "banana"]
thislist.sort(reverse = True)
print(thislist)

# COMMAND ----------

thislist = [100, 50, 65, 82, 23]
thislist.sort(reverse = True)
print(thislist)

# COMMAND ----------

thislist = ["apple", "banana", "cherry"]
mylist = thislist.copy()
print(mylist)

# COMMAND ----------

thislist = ["apple", "banana", "cherry"]
mylist = list(thislist)
print(mylist)

# COMMAND ----------

# This code creates a copy of the list using slicing.
# The slice thislist[:] returns a new list containing all elements of thislist.
# The resulting mylist will be: ['apple', 'banana', 'cherry']

thislist = ["apple", "banana", "cherry"]
mylist = thislist[:]
print(mylist)

# COMMAND ----------

list1 = ["a", "b", "c"]
list2 = [1, 2, 3]

list3 = list1 + list2
print(list3)

# COMMAND ----------

list1 = ["a", "b" , "c"]
list2 = [1, 2, 3]

for x in list2:
  list1.append(x)

print(list1)

# COMMAND ----------

list1 = ["a", "b" , "c"]
list2 = [1, 2, 3]

list1.extend(list2)
print(list1)

# COMMAND ----------

# List Methods and Their Descriptions
# -----------------------------------
# append()   : Adds an element at the end of the list
# clear()    : Removes all the elements from the list
# copy()     : Returns a copy of the list
# count()    : Returns the number of elements with the specified value
# extend()   : Adds the elements of a list (or any iterable) to the end of the current list
# index()    : Returns the index of the first element with the specified value
# insert()   : Adds an element at the specified position
# pop()      : Removes the element at the specified position
# remove()   : Removes the item with the specified value
# reverse()  : Reverses the order of the list
# sort()     : Sorts the list

# COMMAND ----------

# MAGIC %md
# MAGIC #TUPLE

# COMMAND ----------

thistuple = ("apple", "banana", "cherry")
print(thistuple)

# COMMAND ----------

thistuple = ("apple", "banana", "cherry", "apple", "cherry")
print(thistuple)

# COMMAND ----------

thistuple = ("apple", "banana", "cherry")
print(len(thistuple))

# COMMAND ----------

thistuple = ("apple",)
print(type(thistuple))

#NOT a tuple
thistuple = ("apple")
print(type(thistuple))

# COMMAND ----------

tuple1 = ("apple", "banana", "cherry")
tuple2 = (1, 5, 7, 9, 3)
tuple3 = (True, False, False)

# COMMAND ----------

tuple1 = ("abc", 34, True, 40, "male")
print(tuple1)

# COMMAND ----------

#Accessing Tuples
thistuple = ("apple", "banana", "cherry")
print(thistuple[1])

# COMMAND ----------

thistuple = ("apple", "banana", "cherry", "orange", "kiwi", "melon", "mango")
print(thistuple[2:5])

# COMMAND ----------

# MAGIC %md
# MAGIC Once a tuple is created, you cannot change its values. Tuples are unchangeable, or immutable as it also is called.
# MAGIC
# MAGIC But there is a workaround. You can convert the tuple into a list, change the list, and convert the list back into a tuple.

# COMMAND ----------

#Change tuple values
x = ("apple", "banana", "cherry")
y = list(x)
y[1] = "kiwi"
x = tuple(y)

print(x)

# COMMAND ----------

# MAGIC %md
# MAGIC Since tuples are immutable, they do not have a built-in append() method, but there are other ways to add items to a tuple.
# MAGIC
# MAGIC 1.Convert into a list: Just like the workaround for changing a tuple, you can convert it into a list, add your item(s), and convert it back into a tuple.
# MAGIC
# MAGIC 2.Add tuple to a tuple. You are allowed to add tuples to tuples, so if you want to add one item, (or many), create a new tuple with the item(s), and add it to the existing tuple:

# COMMAND ----------

thistuple = ("apple", "banana", "cherry")
y = list(thistuple)
y.append("orange")
thistuple = tuple(y)
print(thistuple)

# COMMAND ----------

#Tuples are unchangeable, so you cannot remove items from it, but you can use the same workaround as we used for changing and adding tuple items:
thistuple = ("apple", "banana", "cherry")
y = list(thistuple)
y.remove("apple")
thistuple = tuple(y)
print(thistuple)


# COMMAND ----------

fruits = ("apple", "banana", "cherry") 

(green, yellow, red) = fruits 

print(green)
print(yellow)
print(red)

# COMMAND ----------

thistuple = ("apple", "banana", "cherry")
for x in thistuple:
  print(x)

# COMMAND ----------

thistuple = ("apple", "banana", "cherry")
for i in range(len(thistuple)):
  print(thistuple[i])

# COMMAND ----------

# MAGIC %md
# MAGIC Loop Through the Index Numbers
# MAGIC

# COMMAND ----------

thistuple = ("apple", "banana", "cherry")
# Loop through the tuple using index numbers
# range(len(thistuple)) generates a sequence of indices: 0, 1, 2
# For each index, access the element at that position in the tuple
# IMPORTANT CELL
for i in range(len(thistuple)):
    print(thistuple[i])

# COMMAND ----------

fruits = ("apple", "banana", "cherry")
mytuple = fruits * 2
print(mytuple)

# COMMAND ----------

# range() generates a sequence of numbers
for i in range(3):
    print(i)  # Output: 0, 1, 2

# count() returns the number of times a value appears in a tuple
thistuple = ("apple", "banana", "apple", "cherry")
print(thistuple.count("apple"))  # Output: 2

# index() returns the first index of a value in a tuple
print(thistuple.index("banana"))  # Output: 1

# COMMAND ----------

fruits = ('apple', 'banana', 'cherry')
# Unpack the first element into x, and the rest into y as a list
(x, *y) = fruits
print(y)  # Output: ['banana', 'cherry']

# COMMAND ----------

fruits = ['apple', 'banana', 'cherry']
for x in fruits:
    print(x)

# COMMAND ----------

fruits = ['apple', 'banana', 'cherry'] 
for x in range(len(fruits)):
    print(fruits)

# COMMAND ----------

fruits = ['apple', 'banana', 'cherry'] 
for x in range(len(fruits)):
    print(x)

# COMMAND ----------

# MAGIC %md
# MAGIC #SET

# COMMAND ----------

# Sets are collections that are unordered, unchangeable (but you can add/remove items), and do not allow duplicate values
thisset = {"apple", "banana", "cherry"}

# Loop through the set
for x in thisset:
    print(x)

# COMMAND ----------

thisset = {"apple", "banana", "cherry"}

thisset.add("orange")

print(thisset)

# COMMAND ----------

#in set, the new items are added  through Updtae
thisset = {"apple", "banana", "cherry"}
tropical = {"pineapple", "mango", "papaya"}

thisset.update(tropical)

print(thisset)

# COMMAND ----------

thisset = {"apple", "banana", "cherry"}
mylist = ["kiwi", "orange"]

thisset.update(mylist)

print(thisset)

# COMMAND ----------

hisset = {"apple", "banana", "cherry"}

thisset.remove("banana")

print(thisset)

# COMMAND ----------

thisset = {"apple", "banana", "cherry"}

thisset.discard("banana")

print(thisset)

# COMMAND ----------

thisset = {"apple", "banana", "cherry"}

thisset.clear()

print(thisset)

# COMMAND ----------

thisset = {"apple", "banana", "cherry"}

del thisset

print(thisset)

# COMMAND ----------

thisset = {"apple", "banana", "cherry"}

for x in thisset:
  print(x)

# COMMAND ----------

# MAGIC %md
# MAGIC The union() and update() methods joins all items from both sets.
# MAGIC
# MAGIC The intersection() method keeps ONLY the duplicates.
# MAGIC
# MAGIC The difference() method keeps the items from the first set that are not in the other set(s).
# MAGIC
# MAGIC The symmetric_difference() method keeps all items EXCEPT the duplicates.
# MAGIC
# MAGIC

# COMMAND ----------

set1 = {"a", "b", "c"}
set2 = {1, 2, 3}

set3 = set1.union(set2)
print(set3)

# COMMAND ----------

set1 = {"a", "b", "c"}
set2 = {1, 2, 3}

set3 = set1 | set2
print(set3)

# COMMAND ----------

set1 = {"a", "b", "c"}
set2 = {1, 2, 3}
set3 = {"John", "Elena"}
set4 = {"apple", "bananas", "cherry"}

myset = set1.union(set2, set3, set4)
print(myset)

# COMMAND ----------

set1 = {"a", "b", "c"}
set2 = {1, 2, 3}
set3 = {"John", "Elena"}
set4 = {"apple", "bananas", "cherry"}

myset = set1 | set2 | set3 |set4
print(myset)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Set Methods
# MAGIC
# MAGIC | Method                     | Shortcut | Description                                                                 |
# MAGIC |----------------------------|----------|-----------------------------------------------------------------------------|
# MAGIC | `add()`                    |          | Adds an element to the set                                                  |
# MAGIC | `clear()`                  |          | Removes all the elements from the set                                       |
# MAGIC | `copy()`                   |          | Returns a copy of the set                                                   |
# MAGIC | `difference()`             | -        | Returns a set containing the difference between two or more sets            |
# MAGIC | `difference_update()`      | -=       | Removes the items in this set that are also included in another set         |
# MAGIC | `discard()`                |          | Removes the specified item                                                  |
# MAGIC | `intersection()`           | &        | Returns a set that is the intersection of two other sets                    |
# MAGIC | `intersection_update()`    | &=       | Removes the items in this set not present in other specified set(s)         |
# MAGIC | `isdisjoint()`             |          | Returns whether two sets have an intersection or not                        |
# MAGIC | `issubset()`               | <=       | Returns True if all items of this set are present in another set            |
# MAGIC |                            | <        | Returns True if all items of this set are present in another, larger set    |
# MAGIC | `issuperset()`             | >=       | Returns True if all items of another set are present in this set            |
# MAGIC |                            | >        | Returns True if all items of another, smaller set are present in this set   |
# MAGIC | `pop()`                    |          | Removes an element from the set                                             |
# MAGIC | `remove()`                 |          | Removes the specified element                                               |
# MAGIC | `symmetric_difference()`   | ^        | Returns a set with the symmetric differences of two sets                    |
# MAGIC | `symmetric_difference_update()` | ^=  | Inserts the symmetric differences from this set and another                 |
# MAGIC | `union()`                  | \|       | Returns a set containing the union of sets                                  |
# MAGIC | `update()`                 | \|=      | Updates the set with the union of this set and others                       |
