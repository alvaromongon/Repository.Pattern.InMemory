[![Build Status](https://dev.azure.com/wtwd/Ease%20Maker/_apis/build/status/Repository.Pattern.Azure.Storage.Table?branchName=master)](https://dev.azure.com/wtwd/Ease%20Maker/_build/latest?definitionId=3&branchName=master)

# Introduction 
Repository pattern implementation for an in memory concurrent dictionary.
This is very useful when working on POCs where you are going to use later other repository pattern implementations.

# Getting Started
Just get it and build it. I was using VS 2019

The idea is to have a separate IRepository for each domain object. 
The domain object will know nothing about the storage layer and the effort to build the storage layer is minimum.

# Build and Test
Building is easy since there it not special dependecies.
There are no tests since this is a simple not productive piece of code
