package com.jnorthr;

// see: http://mrhaki.blogspot.fr/2011/09/groovy-goodness-using-named-ordinal.html
/*
@GrabConfig(systemClassLoader=true)
@Grab(group='com.h2database', module='h2', version='1.4.185')
@Grab(group='org.slf4j', module='slf4j-api', version='1.6.1')
@Grab(group='ch.qos.logback', module='logback-classic', version='0.9.28')
*/
import groovy.util.logging.Slf4j
import groovy.sql.Sql

/**
 * Represents an abstraction against an instance of the H2 in-memory database.
 * Used to persist and order info. within a single table.
 */
@Slf4j
public class H2{

    /** Name of the H2 database to be established */
    String dbname ="~/data";

    /** An instance of the sql statement processor */
    Sql sql;

    /** Name of the table to be established within this H2 database */
    String tablename ="data";

    /** A flag set true after the first use of the tablename. Used to avoid issues where tablename is changed after database already in use. */
    boolean tableOpen = false;

    /** A value of the row number of an sql statement where a multi-row paging activity begins; see: http://mrhaki.blogspot.fr/2011/05/groovy-goodness-paging-support-in.html*/
    private int startAtRow = 1;

    /** A value of the total number of rows from a multi-row paging activity to be returned in the result set; see: http://mrhaki.blogspot.fr/2011/05/groovy-goodness-paging-support-in.html*/
    private int howManyRows = 10;
    
   /**
   * Creates an instance of the H2 in-memory database. The database container name defaults to 'data'.
   *
   * The user.home location is included in the default database name.
   */
    public H2()
    {
       sql = Sql.newInstance("jdbc:h2:mem:${dbname}", "sa", "sa", "org.h2.Driver")
    } // end of constructor
    
   /**
   * Creates a specific instance of the H2 in-memory database with the given name.
   *
   * It creates an H2 database name from the String value given to the this constructor.
   * The ~ tilde character is required to identify the user.home location of the declared database.
   *
   * An example database name of '~/fred' would create a temporary database named 'fred' in the user.home folder.
   * @param  dbtable - the name to be used for this instance of an H2 database
   */
    public H2(String dbtable)
    {
       this.dbname = dbtable;
       sql = Sql.newInstance("jdbc:h2:mem:${dbname}", "sa", "sa", "org.h2.Driver")
    } // end of constructor
    
    
    /**
     * Returns tablename for submitted sql statements.
     *
     * The cmd argument must be a valid H2 name. For example:
     * h2.setTableName("fred")
     * 
     * @param  newtablename  the table name to be used against the currently unopened database
     * @return tablename to be used by database if the command was successful
     *
     * @throws IOException when attempt is made to change tablename but database is already open and table is in use
    */
    public setTableName(String newtablename)
    {
        if (tableOpen==true) {throw new IOException("cannot change tablename after database has been used")};
        log.info "setting db table name to "+newtablename;
        this.tablename = newtablename;
    } // end of method
    

    /**
     * Establish values for multi-row paging algorithm. This is typically for an sql select stmt.
     *
     * @param  int startAt a base-1 value of the number of the row as the first row to be taken from a multi-row select
     * @param  int howMany a value of the maximum number of the rows to be returned beginning from the 'startAt' row.
     *
     * @return void
     *
     * @throws IOException when attempt is made to change tablename but database is already open and table is in use
    */
    public setPaging(int startAt, int howMany)
    {
        log.info "set row paging parms";
        startAtRow = startAt;
        howManyRows = howMany
    } // end of method
    

    /**
     * Executes the submitted sql statement using sql.execute(cmd).
     *
     * The cmd argument must be a valid H2 sql statement. Examples include most DDL choices. For example:
     * "create table if not exists test (id int, value char)"
     * 
     * @param  cmd  the sql statement to be executed against the currently open table
     * @return      count of affected rows if the command was successful
    */
    public run(String cmd)
    {
        logmsg("run", "execute",cmd, false)
        tableOpen=true;
        sql.execute(cmd)
        return sql.updateCount;
    } // end of method

    public logmsg(String method, String sqlcmd, String cmd, boolean flag)
    {
        def msg = "running $method for sql stmt $sqlcmd; the cmd is $cmd "
        msg+=(flag)?"with map":"";
        log.debug msg;
    } // end of method
    

    /**
     * Executes the submitted sql statement using sql.execute(cmd,map).
     *
     * The cmd argument must be a valid H2 sql statement. Examples include most DDL choices. For example:
     * h2.run("delete from ${h2.tablename} where id > ?1.id",[id:31])
     * 
     * @param  cmd  the sql statement to be executed against the currently open table
     * @param  map  sql replacement parameters in the form of [id:78] as Map
     * @return      count of affected rows if the command was successful
    */
    public run(String cmd, Map map)
    {
        logmsg("run", "execute",cmd, true)
        log.debug "running run execute cmd with map ="+cmd;
        tableOpen=true;
        sql.execute(cmd, map)
        return sql.updateCount;
    } // end of method

    /**
     * Executes the submitted sql statement using sql.executeUpdate(cmd).
     *
     * The cmd argument must be a valid H2 sql statement. Examples include most update choices. For example:
     * "update test set value='Fred' where id=1"
     * 
     * Any needed parameters are declared within the command.
     * 
     * @param  cmd  the sql update statement to be used against the currently open table
     * @return      count of affected rows if the command was successful
    */
    public update(String cmd)
    {
        tableOpen=true;
        logmsg("update", "executeUpdate",cmd, false)
        def result = sql.executeUpdate(cmd)
        return sql.updateCount;
    } // end of method

    /**
     * Executes the submitted sql statement using sql.executeUpdate(cmd,map).
     *
     * The cmd argument must be a valid H2 sql statement. Examples include most update choices. For example:
     * "update test set value='Frank' where id=11"
     * 
     * Any needed sql replacement parameters are provided with a map passed in as the second parm.
     * 
     * @param  cmd  the sql update statement to be used against the currently open table
     * @param  map  sql replacement parameters in the form of [id:76] as Map
     * @return      count of affected rows if the command was successful
    */
    public update(String cmd, Map map)
    {
        tableOpen=true;
        logmsg("update", "executeUpdate",cmd, true)
        def result = sql.executeUpdate(cmd,map)
        return sql.updateCount;
    } // end of method
    

    /**
     * Executes the submitted sql statement using sql.executeInsert(cmd).
     *
     * The cmd argument must be a valid H2 sql statement. Examples include most update choices. For example:
     * "insert into test set value='Frank' where id=11"
     * 
     * Any needed sql replacement parameters are provided with a map passed in as the second parm.
     * 
     * @param  cmd  the sql executeInsert statement to be used against the currently open table
     * @return      id's of inserted rows if the command was successful
    */
    public insert(String cmd)
    {
        tableOpen=true;
        logmsg("insert", "executeInsert",cmd, false)
        log.debug "running executeInsert cmd ="+cmd;
        def insertedIds = sql.executeInsert(cmd)
        return insertedIds;
    } // end of method

    /**
     * Executes the submitted sql statement using sql.executeInsert(cmd,map).
     *
     * The cmd argument must be a valid H2 sql insert statement. Examples include most update choices. For example:
     * h2.insert("insert into ${h2.tablename} values(:id, :value)", [id: 41, value: 'hello 41'])
     * 
     * Any needed sql replacement parameters are provided with a map passed in as the second parm.
     * 
     * @param  cmd  the sql update statement to be used against the currently open table
     * @param  map  sql replacement parameters in the form of [id:76] as Map
     * @return      id's of inserted rows if the command was successful
    */
    public insert(String cmd, Map map)
    {
        tableOpen=true;
        logmsg("insert", "executeInsert",cmd, true)
        def insertedIds = sql.executeInsert(cmd,map)
        return insertedIds;
    } // end of method


    /**
     * Executes the submitted sql statement using sql.execute(cmd,map).
     *
     * The cmd argument must be a valid H2 sql insert statement. Examples include several choices choices. For example:
     * h2.add("insert into ${h2.tablename} values(:id, :value)", [id: 41, value: 'hello 41'])
     * 
     * Any needed sql replacement parameters are provided with a map passed in as the second parm.
     * 
     * @param  cmd  the sql statement to be used against the currently open table
     * @param  map  sql replacement parameters in the form of [id:76] as Map
     * @return      count of affected rows if the command was successful
    */
    public add(String cmd, Map map)
    {
        tableOpen=true;
        logmsg("add", "execute",cmd, true)
        sql.execute(cmd,map)
        return sql.updateCount;
    } // end of method

    /**
     * Executes the submitted sql statement using sql.execute(cmd,map). 
     *
     * The cmd argument must be a valid H2 sql insert statement. Examples include most update choices. For example:
     * h2.put("insert into ${h2.tablename} values(:id, :value)", [id: 41, value: 'hello 41'])
     * 
     * Any needed sql replacement parameters are provided with a map passed in as the second parm.
     * 
     * @param  cmd  the sql statement to be used against the currently open table
     * @param  map  sql replacement parameters in the form of [id:76] as Map
     * @return      count of affected rows if the command was successful
    */
    public put(String cmd, Map map)
    {
        tableOpen=true;
        logmsg("put", "execute",cmd, true)
        sql.execute(cmd,map)
        return sql.updateCount;
    } // end of method


    /**
     * Executes the sql select statement using sql.rows(cmd). 
     *
     * The cmd argument must be a valid H2 sql select statement. Examples include 'where' and 'order by' choices. For example:
     * h2.get("select * from ${h2.tablename} where id > 43 order by value")
     * 
     * @param  cmd  the sql statement to be used against the currently open table
     * @return      array of matching rows if the command was successful
    */
    public get(String cmd)
    {
        tableOpen=true;
        logmsg("get", "rows",cmd, false)
        return sql.rows(cmd)
    } // end of method


    /**
     * Executes the sql select statement using sql.rows(cmd,map,closure) passing each selected row to the provided closure logic. 
     *
     * The cmd argument must be a valid H2 sql select statement. Examples include 'where' and 'order by' choices. For example:
     * h2.get("select * from ${h2.tablename} where id > ?1.id order by value",[id:24],{e-> println e.id; })
     * 
     * Processing logic to be applied to each row is declared in a named or anonymous closure
     * 
     * @param  cmd  the sql statement to be used against the currently open table
     * @param  map  sql replacement parameters in the form of [id:76] as Map
     * @param  logic named or anonymous closure to be applied to each row
     * @return      array of matching rows if the command was successful
    */
    public get(String cmd, Map map, Closure logic)
    {
        tableOpen=true;
        logmsg("get", "rows into closure",cmd, true)
        log.debug "running get closure with map cmd="+cmd;    
        return logic(sql.rows(cmd,map))
    } // end of method


    /**
     * Executes the sql select statement using sql.rows(cmd,closure) passing each selected row to the provided closure logic. 
     *
     * The cmd argument must be a valid H2 sql select statement. Examples include 'where' and 'order by' choices. For example:
     * h2.get("select * from ${h2.tablename} where id > 47 order by value",{e-> })
     * 
     * Processing logic to be applied to each row is declared in a named or anonymous closure
     * 
     * @param  cmd  the sql statement to be used against the currently open table
     * @param  logic named or anonymous closure to be applied to each row
     * @return      array of matching rows if the command was successful
    */
    public get(String cmd, Closure logic)
    {
        tableOpen=true;
        logmsg("get", "rows into closure",cmd, false)
        return logic(sql.rows(cmd))
    } // end of method

    /**
     * Executes the submitted sql statement using sql.rows(cmd,map). 
     *
     * The cmd argument must be a valid H2 sql select statement. Examples include 'where' and 'order by' choices. For example:
     * h2.get("select * from ${h2.tablename} where id > ?1.id order by value",[id:44])
     * 
     * Any needed sql replacement parameters are provided with a map passed in as the second parm.
     * 
     * @param  cmd  the sql statement to be used against the currently open table
     * @param  map  sql replacement parameters in the form of [id:76] as Map
     * @return      array of matching rows if the command was successful
    */
    public get(String cmd, Map map)
    {
        tableOpen=true;
        logmsg("get", "rows",cmd, true)
        return sql.rows(cmd,map);
    } // end of method
    

    /**
     * Executes the submitted sql statement using sql.rows(cmd,map,map). 
     *
     * The cmd argument must be a valid H2 sql select statement. Examples include 'where' and 'order by' choices. For example:
     * h2.get("select * from ${h2.tablename} where id > 0 order by value")
     * 
     * Any needed sql replacement parameters are provided with maps passed in as the second and third parms.
     * 
     * @param  cmd  the sql statement to be used against the currently open table
     * @param  map  sql replacement parameters in the form of [id:76] as Map
     * @param  map  sql replacement parameters in the form of [value:'Fred'] as Map
     * @return      array of matching rows if the command was successful
    */
    public get(String cmd, Map map1, Map map2)
    {
        tableOpen=true;
        logmsg("get", "rows+2 maps",cmd, true)
        return sql.rows(cmd,map1,map2);
    } // end of method


   /** 
    * Method to run class tests.
    * 
    * @param args Value is string array - possibly empty - of command-line values. 
    * @return void
    */    
    public static void main(String[] args) 
    {
        Closure loop = {it.each{e-> log.info "${e.toString()}" } } 

        log.info "--- the start ---"
        H2 h2 = new H2("~/info");
        h2.setTableName "junk";
        
        //def ans = h2.sql.execute("drop table if exists test")
        def ans = h2.run("drop table if exists ${h2.tablename}")
        log.info "ans=${ans}"

        //ans = h2.sql.execute("create table if not exists test (id int, value char)")
        ans = h2.run("create table if not exists ${h2.tablename} (id int not null, value char)");
        log.info "created ${h2.tablename} ... ans=${ans}"

        ans = h2.run("delete from ${h2.tablename}");
        log.info "did delete all from ${h2.tablename} ... ans=${ans}"

        //h2.sql.execute("create unique index if not exists ix1 on test(id)")
        ans = h2.run("create unique index if not exists ix1 on ${h2.tablename}(id)");
        log.info "created unique index ix1 on ${h2.tablename} ... ans=${ans}"

        log.info "----------------------\n"

        // add some rows
        ans = h2.add("insert into ${h2.tablename} values(:id, :value)", [id: 43, value: 'hello 43'])
        log.info "ans=${ans}"

        ans = h2.insert("insert into ${h2.tablename} values(:id, :value)", [id: 41, value: 'hello 41']);        
        log.info "ans=${ans}"

        ans = h2.put("insert into ${h2.tablename} values(:id, :value)", [id: 31, value: 'hello 31'])
        log.info "ans=${ans}"

        ans = h2.insert "insert into ${h2.tablename} values(21, 'hello 21')"
        log.info "ans=${ans}"
        
        ans = h2.put("insert into ${h2.tablename} values(:id, :value)", [id: 11, value: 'hello 11'])
        log.info "ans=${ans}"

        ans = h2.add("insert into ${h2.tablename} values(:id, :value)", [id: 1, value: 'hello 1'])
        log.info "ans=${ans}"


        ans = h2.get("select * from ${h2.tablename} order by id").each{e-> log.info e.toString();}
        log.info "ans=${ans}"

        log.info "----------------------\n"

        ans = h2.get("select * from ${h2.tablename} order by id"){loop(it)}
        log.info "ans=${ans}"
        log.info "----------------------\n"

        log.info "gonna do updates now ..."
        ans = h2.update("update ${h2.tablename} set value='Frank 21' where id=21");
        log.info "ans=${ans}"

        ans = h2.update("update ${h2.tablename} set value='Frank 11' where id=?1.id",[id:11]);
        log.info "ans=${ans}"
        
            
        ans = h2.run("update ${h2.tablename} set value='Fred' where id=1");
        log.info "ans=${ans}"
        
        ans = h2.put("update ${h2.tablename} set value='Fred 31' where id=?1.id",[id:31])
        log.info "updated ${h2.tablename} ... ans=${ans}"

        log.info "gonna do delete of id=31..."
        ans = h2.put("delete from ${h2.tablename} where id > ?1.id",[id:31])
        log.info "ans=${ans}"
        log.info "deleted from ${h2.tablename} where id>31 ... ans=${ans}"


        log.info "gonna do delete of id=31 again to see what happens on missing row..."
        ans = h2.run("delete from ${h2.tablename} where id > ?1.id",[id:31])
        log.info "ans=${ans}"
        log.info "deleted from ${h2.tablename} where id>31 again ?... ans=${ans}"


        ans = h2.get("select * from ${h2.tablename} order by id").each{e-> println "id=${e.id} value=${e.value}"}
        log.info "ans=${ans}"
        log.info "----------------------\n"

        def result = h2.get("select * from ${h2.tablename} where value =:value or id = :id",[value: 'Groovy', id: 1] /* ?1.name */ /*[id: 1]*/ /* ?2.id */)
        result.each{e-> log.info "mrhaki select="+e.id}
        
        def ct=0;
        result = h2.get("select * from ${h2.tablename} where id > 20 order by value").each{e->  println "id=${e.id} value=${e.value}"; ct+=e.id; };
        log.info "result=${result} and ct=$ct"

        log.info "--- the end ---"
  } // end of main

} // end of class