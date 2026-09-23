1. Go is a compiled language
2. Strongly typed & statitcally typed
    What does this mean exactly? -> 
3. Fast compilation time (faster than python and javascript of course )
4. Built in Concurrency
    What does this mean exactly? --> 
5. Simple readings apparently 


## Syntax

# Variables, print, and file running
import "packagename" 
-> You must use the import otherwise compilation error

fmt.Println("Message")
--> Prints messages

fmt.Printf("Some message %v", variableInString)
--> Prints message but with a variable 

go run filename 
--> Runs the file

var varName type[bitNumber]
--> Overflow error
--> Think about bit size (recall systems class)

# Functions and ifs
func funcName(param paramType) returnType {

}

Design Pattern -- if your function can encounter an error, you want to return type error. 
var err error

if condition{

}

add "errors" to import and now we can do 

// int1 = quotient, int2 = remainder, error = error handling
func intDivision(numerator int, denominator int) (int, int, error) {
    var err error

    if denominator==0{
        err = errors.New("Cannot divide by zero")
        return 0, 0, err
    }
}

if err!=nil --> There was an error in the function call 


# Arrays, maps, and loops

[]Arrays are fixed length, all same type, 0-indexed, and contiguous in memory
var arrName [size]type[bitSize]

var mapName map[keyType][valueType] = make(map[keyType][valueType])



# Structs in Go:
Go doesn't support OOP classes natively, and so in place of that, go uses structs a lot more 

type StructName struct {
    VarName VarType
    VarName VarType
    ...

}


accesssing:
fmt.Println(StructName.VarName) 
--> Prints the value of the variable name (if not set, uses default values)

Unlike classes, structs cannot contain function decalrations / definitions inside of them 

So, you make something called a receiver function outside of it (either passing in a pointer or you pass in the struct by value)
Pointer --> Large struct, contains Synchronizer, or you need to mutate struct data directly
Value --> Small, immutable, simple. 

BEST PRACTICE --> One struct should have uniform functions (either all PBV or PBR)


# File structure:
cmd/ is specifically for community convention so stuff like entry points (package main)
- Entry points
- CLI tools 
- Package type is strictly package main
- Not meant to be imported, it imports other packages 

internal/ is specifically for compiler mechanism to make sure the code remains private and cannot be imported by external modules 
- Private business logic and implementation details
- enforced by Go compiler
- Package type is any library package name 
- Can only be imported by packages inside the same module 