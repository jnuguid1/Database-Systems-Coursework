/*
    Name: Jared Nuguid
    Student Number: 216 316 143
    Date: 12/08/2020
*/

import java.util.*;
import java.net.*;
import java.text.*;
import java.lang.*;
import java.io.*;
import java.sql.*;
import pgpass.*;
import java.sql.Date;
import java.time.LocalDate;

public class CreateQuest {
    private Connection conDB;        // Connection to the database system.
    private String url;              // URL to the Database
    private String user = "jared1";  // Database user account

    private Date date;
    private String realm;
    private String theme;
    private int amount;
    private double seed;

    // Constructor
    public CreateQuest (String[] args) {
        //Set up the DB connection
        try {
            //Register the driver with DriverManager
            Class.forName("org.postgresql.Driver").newInstance();
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
            System.exit(0);
        } catch (InstantiationException e) {
            e.printStackTrace();
            System.exit(0);
        } catch (IllegalAccessException e) {
            e.printStackTrace();
            System.exit(0);
        }

        // URL: Which database?
        url = "jdbc:postgresql://db:5432/";

        if (args.length > 4) {
            try {
                user = args[4];
            } catch (IllegalArgumentException e) {
                System.out.println("user must be a String.");
                System.exit(0);
            }
        }

        // set up acct info
        // fetch the PASSWD from <.pgpass>
        Properties props = new Properties();
        try {
            String passwd = PgPass.get("db", "*", user, user);
            props.setProperty("user",  user);
            props.setProperty("password", passwd);
        } catch(PgPassException e) {
            System.out.print("\nCould not obtain PASSWD from <.pgpass>.\n");
            System.out.println(e.toString());
            System.exit(0);
        }

        // Initialize the connection.
        try {
            conDB = DriverManager.getConnection(url, props);
        } catch(SQLException e) {
            System.out.print("\nSQL: database connection error.\n");
            System.out.println(e.toString());
            System.exit(0);
        }
        
        if (args.length != 4 && args.length != 6) {
            System.out.println("\nIncorrect input");
            System.out.println("Usage: java CreateQuest <day> <realm> <theme> <amount> [<user>] [seed]");
            System.exit(0);
        } else {
            if (args.length == 4) {
                try {
                    date = Date.valueOf(args[0]);
                    realm = args[1];
                    theme = args[2];
                    amount = Integer.parseInt(args[3]);    
                } catch (IllegalArgumentException e) {
                    System.out.println("\nUsage: java CreateQuest <day> <realm> <theme> <amount> [<user>] [seed]");
                    System.exit(0);
                }
            } else if (args.length == 6) {
                try {
                    date = Date.valueOf(args[0]);
                    realm = args[1];
                    theme = args[2];
                    amount = Integer.parseInt(args[3]);
                    seed = Double.parseDouble(args[5]);
    
                } catch (IllegalArgumentException e) {
                    System.out.println("\nUsage: java CreateQuest <day> <realm> <theme> <amount> [<user>] [seed]");
                    System.exit(0);
                }
            }
        }

        // Check if the realm exists in the RR Database.
        if (!realmCheck()) {
            System.out.println("There is no realm named " + realm + " in the database.");
            System.exit(0);
        }

        // Check if the inputted date is a future date.
        if (date.toLocalDate().isBefore(LocalDate.now())) {
            System.out.println("The inputted date must be a future date.");
            System.exit(0);
        }

        // Check if the amount entered exceeds what is possible
        if (!checkSum()) {
            System.out.println("Amount exceeds what is possible.");
            System.exit(0);
        }

        addQuest();

        // Check if the seed value is proper.
        if (seed < -1 || seed > 1) {
            System.out.println("Seed value is improper. It must be between -1 and 1.");
            System.exit(0);
        }

        if (args.length == 6) {
            setSeed();
        }

        randomTreasure();

        // Close the connection
        try {
            conDB.close();
        } catch(SQLException e) {
            System.out.print("\nFailed trying to close the connection.\n");
            e.printStackTrace();
            System.exit(0);
        }
    }

    public boolean realmCheck() {
        String            queryText = "";    // The SQL text.
        PreparedStatement querySt   = null;  // The query handle.
        ResultSet         answers   = null;  // A cursor.
        boolean           inDB      = false; //Return

        queryText = 
           "SELECT realm     "
         + "FROM Realm       "
         + "WHERE realm = ? ;";
        
        // Prepare the query.
        try {
            querySt = conDB.prepareStatement(queryText);
        } catch(SQLException e) {
            System.out.println("SQL#1 failed in prepare");
            System.out.println(e.toString());
            System.exit(0);
        }

        // Execute the query.
        try {
            querySt.setString(1, realm);
            answers = querySt.executeQuery();
        } catch(SQLException e) {
            System.out.println("SQL#1 failed in execute");
            System.out.println(e.toString());
            System.exit(0);
        }

        try {
            if (answers.next()) {
                inDB = true;
            } else {
                inDB = false;
            }
        } catch(SQLException e) {
            System.out.println("SQL#1 failed in cursor.");
            System.out.println(e.toString());
            System.exit(0);
        }

        // Close the cursor.
        try {
            answers.close();
        } catch(SQLException e) {
            System.out.print("SQL#1 failed closing cursor.\n");
            System.out.println(e.toString());
            System.exit(0);
        }

        // Close the handle.
        try {
            querySt.close();
        } catch(SQLException e) {
            System.out.print("SQL#1 failed closing the handle.\n");
            System.out.println(e.toString());
            System.exit(0);
        }

        return inDB;
    }

    public int addQuest() {
        String            queryText = "";    // The SQL text.
        PreparedStatement querySt   = null;  // The query handle.
        int num = 0;

        queryText = 
            "INSERT INTO Quest(theme, realm, day, succeeded) values"
          + "   (?, ?, ?, NULL);                                   ";
        
        // Prepare the query.
        try {
            querySt = conDB.prepareStatement(queryText);
        } catch(SQLException e) {
            System.out.println("SQL#2 failed in prepare");
            System.out.println(e.toString());
            System.exit(0);
        }

        // Execute the query.
        try {
            querySt.setString(1, theme);
            querySt.setString(2, realm);
            querySt.setDate(3, date);
            num = querySt.executeUpdate();
        } catch(SQLException e) {
            System.out.println("SQL#2 failed in execute");
            System.out.println(e.toString());
            System.exit(0);
        }

        // Close the handle.
        try {
            querySt.close();
        } catch(SQLException e) {
            System.out.print("SQL#2 failed closing the handle.\n");
            System.out.println(e.toString());
            System.exit(0);
        }
        return num;
    }

    public boolean checkSum() {
        String            queryText = "";    // The SQL text.
        PreparedStatement querySt   = null;  // The query handle.
        ResultSet         answers   = null;  // A cursor.
        int               sum       = 0;     // sum of the scrip of the treasures listed in Treasure
        boolean           inDB      = false; // return

        queryText = 
            "SELECT SUM(sql) as total " +
            "FROM Treasure           ;" ;
        
        try {
            querySt = conDB.prepareStatement(queryText);
        } catch(SQLException e) {
            System.out.println("SQL#3 failed in prepare");
            System.out.println(e.toString());
            System.exit(0);
        }

        try {    
            answers = querySt.executeQuery();
        } catch(SQLException e) {
            System.out.println("SQL#3 failed in execute");
            System.out.println(e.toString());
            System.exit(0);
        }

        try {
            answers.next();
            sum = answers.getInt("total");
            if (amount <= sum)
                inDB = true;
            else
                inDB = false;
        } catch(SQLException e) {
            System.out.println("SQL#3 failed in cursor.");
            System.out.println(e.toString());
            System.exit(0);
        }

        // Close the cursor.
        try {
            answers.close();
        } catch(SQLException e) {
            System.out.print("SQL#3 failed closing cursor.\n");
            System.out.println(e.toString());
            System.exit(0);
        }

        // Close the handle.
        try {
            querySt.close();
        } catch(SQLException e) {
            System.out.print("SQL#3 failed closing the handle.\n");
            System.out.println(e.toString());
            System.exit(0);
        }

        return inDB;
    } 

    public void randomTreasure() {
        String            queryText = "";    // The SQL text.
        PreparedStatement querySt   = null;  // The query handle.
        ResultSet         answers   = null;  // A cursor.
        int               total     = 0;     // The total amount of sql attached to the quest so far.
        int               lootid    = 1;

        queryText = 
            "SELECT *          " +
            "FROM Treasure     " +
            "ORDER BY RANDOM();";

        try {
            querySt = conDB.prepareStatement(queryText);
        } catch(SQLException e) {
            System.out.println("SQL#4 failed in prepare");
            System.out.println(e.toString());
            System.exit(0);
        }

        try {    
            answers = querySt.executeQuery();
        } catch(SQLException e) {
            System.out.println("SQL#4 failed in execute");
            System.out.println(e.toString());
            System.exit(0);
        }

        String treasure;
        int sql;

        try {
            do {
                answers.next();
                treasure = answers.getString("treasure");
                sql = answers.getInt("sql");
                total += sql;
                addLoot(lootid, treasure);
                lootid++;
            } while (total < amount);    
        } catch(SQLException e) {
            System.out.println("SQL#4 failed in cursor.");
            System.out.println(e.toString());
            System.exit(0);
        }

        // Close the cursor.
        try {
            answers.close();
        } catch(SQLException e) {
            System.out.print("SQL#4 failed closing cursor.\n");
            System.out.println(e.toString());
            System.exit(0);
        }

        // Close the handle.
        try {
            querySt.close();
        } catch(SQLException e) {
            System.out.print("SQL#4 failed closing the handle.\n");
            System.out.println(e.toString());
            System.exit(0);
        }
    }

    public void addLoot(int lootid, String treasure) {
        String            queryText = "";    // The SQL text.
        PreparedStatement querySt   = null;  // The query handle.
        ResultSet         answers   = null;  // A cursor.

        queryText = 
            "INSERT INTO Loot(loot_id, treasure, theme, realm, day, login) values"
          + "   (?, ?, ?, ?, ?, NULL);                                         ;";
        
        // Prepare the query.
        try {
            querySt = conDB.prepareStatement(queryText);
        } catch(SQLException e) {
            System.out.println("SQL#5 failed in prepare");
            System.out.println(e.toString());
            System.exit(0);
        }

        // Execute the query.
        try {
            querySt.setInt(1, lootid);
            querySt.setString(2, treasure);
            querySt.setString(3, theme);
            querySt.setString(4, realm);
            querySt.setDate(5, date);
            querySt.executeUpdate();
        } catch(SQLException e) {
            System.out.println("SQL#5 failed in execute");
            System.out.println(e.toString());
            System.exit(0);
        }

        // Close the handle.
        try {
            querySt.close();
        } catch(SQLException e) {
            System.out.print("SQL#5 failed closing the handle.\n");
            System.out.println(e.toString());
            System.exit(0);
        }
    }

    public void setSeed() {
        String            queryText = "";    // The SQL text.
        PreparedStatement querySt   = null;  // The query handle.
        ResultSet         answers   = null;  // A cursor.

        queryText = 
            "SELECT SETSEED(?);";
        
        // Prepare the query.
        try {
            querySt = conDB.prepareStatement(queryText);
        } catch(SQLException e) {
            System.out.println("SQL#6 failed in prepare");
            System.out.println(e.toString());
            System.exit(0);
        }

        // Execute the query.
        try {
            querySt.setDouble(1, seed);
            querySt.executeQuery();
        } catch(SQLException e) {
            System.out.println("SQL#6 failed in execute");
            System.out.println(e.toString());
            System.exit(0);
        }

        // Close the handle.
        try {
            querySt.close();
        } catch(SQLException e) {
            System.out.print("SQL#6 failed closing the handle.\n");
            System.out.println(e.toString());
            System.exit(0);
        }
    }

    public static void main(String[] args) {
        CreateQuest cq = new CreateQuest(args);
    }
}