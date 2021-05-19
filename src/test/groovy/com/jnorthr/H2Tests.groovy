package com.jnorthr;
import org.springframework.boot.test.system.OutputCaptureRule;

/*
 * Copyright 2015 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

class H2Test extends spock.lang.Specification {
    H2 h2;

	@org.junit.Rule
	OutputCaptureRule capture = new OutputCaptureRule()

	// run before the first feature method
	def setupSpec() 
	{
	} // end of setupSpec()     

	// run before every feature method
	def setup() 
	{
		h2 = new H2();
	}          

	// run after every feature method
	def cleanup() 
	{

	}        

	// run after the last feature method	
	def cleanupSpec() 
	{

	}   
 
  	def "Build default H2"() {
  		when:   'default H2 has been built'
    	then:   h2 != null;
    			h2.sql != null;
                h2.dbname == "~/data" 
                h2.tablename == "data"
                h2.tableOpen == false
                h2.startAtRow == 1;
                h2.howManyRows == 10; 
  	} // end of feature method
 
  	def "Build non-default H2"() {
  		when : 	'set log flag and print something'
              	h2 = new H2("~/Fred");
 
  		then: 	h2.dbname == "~/Fred" 
              	h2.sql != null;
                h2.tablename == "data"
                h2.tableOpen == false
                h2.startAtRow == 1;
                h2.howManyRows == 10; 
  	} // end of feature method

    def "Built default H2 then set table name()"() {
    	when:   'default H2 has been built and we set setTableName'
    			def name = h2.setTableName("Something");
    	then:   h2.tablename=="Something";
    			h2.tableOpen==false;
    			name=="Something"
    			capture.toString().endsWith("setting db table name to Something\n")
    } // end of feature method


    def "Built default H2 then set sql cursor scroll paging rates"() {
      when:     'default H2 built, now set paging rates'
                int max = h2.setPaging(1, 20);

      then:   	capture.toString().endsWith("set row paging parms\n");         
              	max == 20;
              	h2.startAtRow == 1;
              	h2.howManyRows == 20;
    } // end of feature method

    def "Built default H2 then do a method call a non-existent statement"() {
      when:     'default H2 built, now try calling non-existent method'
                int count = h2.run();

      then:  	thrown org.codehaus.groovy.runtime.metaclass.MethodSelectionException
				capture.toString() == "";
				count==0;         
    } // end of feature method

    def "Built default H2 then do a method call an empty statement"() {
      when:     'default H2 built, now call method with empty statement'
                int count = h2.run("");

      then:  	notThrown org.codehaus.groovy.runtime.metaclass.MethodSelectionException
				capture.toString().endsWith("com.jnorthr.H2 - running run for sql stmt execute; the cmd is  \n");
				count==0;         
    } // end of feature method


    def "Built default H2 then do a method call with a partial statement"() {
      when:     'default H2 built, now try sql statement  with bad syntax'
                int count = h2.run("select ");

      then:  	thrown org.h2.jdbc.JdbcSQLException 
				capture.toString().contains("DEBUG com.jnorthr.H2 - running run for sql stmt execute; the cmd is select")
				count==0;         
    } // end of feature method


    def "Built default H2 then do a method call with a good sql select statement but bad tablename"() {
      when:     'default H2 built, now try sql statement with good select syntax'
                int count = h2.run("select * from data");

      then:  	org.h2.jdbc.JdbcSQLException ex = thrown()
      			ex.message=="Table \"DATA\" not found; SQL statement:\nselect * from data [42102-195]"
				capture.toString().contains("DEBUG com.jnorthr.H2 - running run for sql stmt execute; the cmd is select * from data")
				count==0;         
    } // end of feature method

    def "Built default H2 then do a method call with a good sql select statement and h2.tablename"() {
      when:     'default H2 built, now try sql statement with good select syntax'
                int count = h2.run("select * from ${h2.tablename}");

      then:  	org.h2.jdbc.JdbcSQLException ex = thrown()
      			ex.message=="Table \"DATA\" not found; SQL statement:\nselect * from data [42102-195]"
				capture.toString().contains("DEBUG com.jnorthr.H2 - running run for sql stmt execute; the cmd is select * from data")
				count==0;         
    } // end of feature method

    def "Built default H2 then do a method call with a good sql select statement and DATA tablename in uppercase"() {
      when:     'default H2 built, now try sql statement with good select syntax'
                int count = h2.run("select * from DATA ");

      then:  	org.h2.jdbc.JdbcSQLException ex = thrown()
      			//println "ex.message="+ex.message
      			ex.message=="Table \"DATA\" not found; SQL statement:\nselect * from DATA  [42102-195]"
				capture.toString().contains("DEBUG com.jnorthr.H2 - running run for sql stmt execute; the cmd is select * from DATA")
				count==0;         
    } // end of feature method

    def "Built default H2 then do a method call with a good sql select statement and Something tablename "() {
      when:     'default H2 built, now try sql statement with good select syntax'
      			h2 = new H2("~/Fred");
      			def name = h2.setTableName("Something");
                int count = h2.run("select * from ${name}");
 
      then:  	org.h2.jdbc.JdbcSQLException ex = thrown() 
      			ex.message=="Table \"SOMETHING\" not found; SQL statement:\nselect * from Something [42102-195]"
				capture.toString().contains("DEBUG com.jnorthr.H2 - running run for sql stmt execute; the cmd is select * from ")
				count==0;         
    } // end of feature method


    def "Use default H2, create a table then do an sql select statement"() {
      when:     'default H2 built, now try sql statement with good select syntax'
				def ans   = h2.run("create table if not exists ${h2.tablename} (id int not null, value char)")
                def count = h2.run("select * from ${h2.tablename}");
  
      then:  	notThrown org.h2.jdbc.JdbcSQLException 
				capture.toString().contains("DEBUG com.jnorthr.H2 - running run for sql stmt execute; the cmd is select * from ")
				ans != null;
				count==-1;         
    } // end of feature method


    def "Use default H2, create a table then do an sql insert statement"() {
      when:     'default H2 built, now try sql statement with good select syntax'
				def ans   = h2.run("create table if not exists ${h2.tablename} (id int not null, value char)")
		        def result = h2.add("insert into ${h2.tablename} values(:id, :value)", [id: 43, value: 'hello 43'])
  
      then:  	notThrown org.h2.jdbc.JdbcSQLException 
				capture.toString().contains("DEBUG com.jnorthr.H2 - running run for sql stmt execute; the cmd is create table if not exists data (id int not null, value char)")
				ans != null;
				result==1;         
    } // end of feature method
 

    def "Built non-default H2, create a table then do a method call with a good sql select statement using Something tablename "() {
      when:     'default H2 built, now try sql statement with good select syntax'
      			h2 = new H2("~/Fred");
      			def name = h2.setTableName("Something");
				def ans  = h2.run("create table if not exists ${h2.tablename} (id int not null, value char)")
                def count =h2.run("select * from ${name}");
  
      then:  	notThrown org.h2.jdbc.JdbcSQLException 
      			name != null;
      			name != ""
          		name==h2.tablename; 
      			//ex.message=="Table \"SOMETHING\" not found; SQL statement:\nselect * from Something [42102-185]"
				capture.toString().contains("DEBUG com.jnorthr.H2 - running run for sql stmt execute; the cmd is select * from ")
				count==-1;         
    } // end of feature method

} // end of class  
