# H2Project

Travis Build Status for Master Branch: [![Build Status](https://travis-ci.org/jnorthr/H2Project.svg?branch=master)](https://travis-ci.org/jnorthr/H2Project)

A short sample Groovy class w/tests to try the H2 in-memory database feature;

Just wanted to give you a starting point to try the H2 in-memory database. I've used the Groovy development language here to setup a set of methods. The whole class named H2 is constructed in the style of a java class with all it's syntax. Ok, it's not exactly a java program. For that you would need to add more syntax.

For this demo, i've tried to use the most common features of the H2 grammar.

## Project Layout

This project is arranged in a folder structure that's compatible with many build systems. In this demo, i've used the gradle build tool. Have also included a full gradle wrapper with required bits and pieces the code needs to work correctly. These are declared within the build.gradle script.

### Folder Layout

* build.gradle - the script gradle uses to go urn the build process
* gradle.properties - influences the gradle build tool
* License - what you can/cannot do with this project
* README.md - this text file
* build/  - a folder of all the pieces constructed when you do a **gradlew build** command
* gradle/ - everything needed to make gradle run on your system without installing gradle; Gradle will self-install any missing jars, code, scripts, etc if your system is connected to the internet the first time you do a gradle build command
* src/
* /src/main/groovy/com/jnorthr/H2.groovy - the only bit of code with samples of what can be done when working with the H2 database
* /src/test/groovy/com/jnorthr/H2Tests.groovy - a series of self-testing methods to confirm the H2.groovy class is running correctly.

## H2 Syntax

[H2 Grammar is described here.](http://www.h2database.com/html/grammar.html)

