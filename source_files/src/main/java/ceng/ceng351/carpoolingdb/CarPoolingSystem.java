package ceng.ceng351.carpoolingdb;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;

public class CarPoolingSystem implements ICarPoolingSystem {

    private static String url = "jdbc:h2:mem:carpoolingdb;DB_CLOSE_DELAY=-1"; // In-memory database
    private static String user = "sa";          // H2 default username   Abdullah Durum 2448330
    private static String password = "";        // H2 default password

    private Connection connection;

    public void initialize(Connection connection) {
        this.connection = connection;
    }

    //Given: getAllDrivers()
    //Testing 5.16: All Drivers after Updating the Ratings
    @Override
    public Driver[] getAllDrivers() {
        List<Driver> drivers = new ArrayList<>();
        
        //uncomment following code slice
        String query = "SELECT PIN, rating FROM Drivers ORDER BY PIN ASC;";

        try {
            PreparedStatement ps = this.connection.prepareStatement(query);
            ResultSet rs = ps.executeQuery();
            while (rs.next()) {
                int PIN = rs.getInt("PIN");
                double rating = rs.getDouble("rating");

                // Create a Driver object with only PIN and rating
                Driver driver = new Driver(PIN, rating);
                drivers.add(driver);
            }
            rs.close();
            ps.close();
        } catch (SQLException e) {
            e.printStackTrace();
        }


        
        return drivers.toArray(new Driver[0]); 
    }

    
    //5.1 Task 1 Create tables
    @Override
    public int createTables() {
        int tableCount = 0;

        String createParticipantsTable =
                "CREATE TABLE Participants (" +
                        "    PIN INT PRIMARY KEY," +
                        "    p_name TEXT," +
                        "    age INT" +
                        ")";

        String createPassengersTable =
                "CREATE TABLE Passengers (" +
                        "    PIN INT PRIMARY KEY," +
                        "    membership_status TEXT," +
                        "    FOREIGN KEY (PIN) REFERENCES Participants(PIN)" +
                        ")";

        String createDriversTable =
                "CREATE TABLE Drivers (" +
                        "    PIN INT PRIMARY KEY," +
                        "    rating DOUBLE," +
                        "    FOREIGN KEY (PIN) REFERENCES Participants(PIN)" +
                        ")";

        String createCarsTable =
                "CREATE TABLE Cars (" +
                        "    CarID INT PRIMARY KEY," +
                        "    PIN INT," +
                        "    color TEXT," +
                        "    brand TEXT," +
                        "    FOREIGN KEY (PIN) REFERENCES Drivers(PIN)" +
                        ")";

        String createTripsTable =
                "CREATE TABLE Trips (" +
                        "    TripID INT PRIMARY KEY," +
                        "    CarID INT," +
                        "    date TEXT," +
                        "    departure TEXT," +
                        "    destination TEXT," +
                        "    num_seats_available INT," +
                        "    FOREIGN KEY (CarID) REFERENCES Cars(CarID)" +
                        ")";

        String createBookingsTable =
                "CREATE TABLE Bookings (" +
                        "    TripID INT," +
                        "    PIN INT," +
                        "    booking_status TEXT," +
                        "    FOREIGN KEY (TripID) REFERENCES Trips(TripID)," +
                        "    FOREIGN KEY (PIN) REFERENCES Passengers(PIN)," +
                        "    PRIMARY KEY (TripID, PIN)" +
                        ")";

        try {
            Statement statement = connection.createStatement();
            statement.addBatch(createParticipantsTable);
            statement.addBatch(createPassengersTable);
            statement.addBatch(createDriversTable);
            statement.addBatch(createCarsTable);
            statement.addBatch(createTripsTable);
            statement.addBatch(createBookingsTable);
            statement.executeBatch();

            tableCount = 6; // Number of tables created
        } catch (SQLException e) {
            e.printStackTrace();
        }

        return tableCount;
    }




    //5.17 Task 17 Drop tables
    @Override
    public int dropTables() {
        int tableCount=0;
        try {
            Statement statement = connection.createStatement();
            String query = "DROP TABLE IF EXISTS Bookings ";
            statement.executeUpdate(query);
            statement.close();
            tableCount++;
        } catch (SQLException e) {
            e.printStackTrace();
        }
        try {
            Statement statement = connection.createStatement();
            String query = "DROP TABLE IF EXISTS Cars";
            statement.executeUpdate(query);
            statement.close();
            tableCount++;
        } catch (SQLException e) {
            e.printStackTrace();
        }
        try {
            Statement statement = connection.createStatement();
            String query = "DROP TABLE IF EXISTS  Drivers";
            statement.executeUpdate(query);
            statement.close();
            tableCount++;
        } catch (SQLException e) {
            e.printStackTrace();
        }
        try {
            Statement statement = connection.createStatement();
            String query = "DROP TABLE IF EXISTS Participants";
            statement.executeUpdate(query);
            statement.close();
            tableCount++;
        } catch (SQLException e) {
            e.printStackTrace();
        }
        try {
            Statement statement = connection.createStatement();
            String query = "DROP TABLE IF EXISTS  Passengers";
            statement.executeUpdate(query);
            statement.close();
            tableCount++;
        } catch (SQLException e) {
            e.printStackTrace();
        }
        try {
            Statement statement = connection.createStatement();
            String query = "DROP TABLE IF EXISTS Trips ";
            statement.executeUpdate(query);
            statement.close();
            tableCount++;
        } catch (SQLException e) {
            e.printStackTrace();
        }



        return tableCount;
    }




    
    //5.2 Task 2 Insert Participants
    @Override
    public int insertParticipants(Participant[] participants) {
        int rowsInserted = 0;
        String query = "INSERT INTO Participants (PIN, p_name, age) VALUES (?, ?, ?)";

        try {
            PreparedStatement preparedStatement = connection.prepareStatement(query);

            for (Participant participant : participants) {
                preparedStatement.setInt(1, participant.getPIN());
                preparedStatement.setString(2, participant.getP_name());
                preparedStatement.setInt(3, participant.getAge());

                preparedStatement.addBatch();
            }

            int[] result = preparedStatement.executeBatch();
            for (int count : result) {
                rowsInserted += (count >= 0) ? 1 : 0;
            }

            preparedStatement.close();
        } catch (SQLException e) {
            e.printStackTrace();
        }

        return rowsInserted;
    }

    
    //5.2 Task 2 Insert Passengers
    @Override
    public int insertPassengers(Passenger[] passengers) {
        int rowsInserted = 0;
        String query = "INSERT INTO Passengers (PIN, membership_status) VALUES (?, ?)";

        try {
            PreparedStatement preparedStatement = connection.prepareStatement(query);

            for (Passenger passenger : passengers) {
                preparedStatement.setInt(1, passenger.getPIN());
                preparedStatement.setString(2, passenger.getMembership_status());

                preparedStatement.addBatch();
            }

            int[] result = preparedStatement.executeBatch();
            for (int count : result) {
                rowsInserted += (count >= 0) ? 1 : 0;
            }

            preparedStatement.close();
        } catch (SQLException e) {
            e.printStackTrace();
        }

        return rowsInserted;
    }


    //5.2 Task 2 Insert Drivers
    @Override
    public int insertDrivers(Driver[] drivers) {
        int rowsInserted = 0;
        String query = "INSERT INTO Drivers (PIN, rating) VALUES (?, ?)";

        try {
            PreparedStatement preparedStatement = connection.prepareStatement(query);

            for (Driver driver : drivers) {
                preparedStatement.setInt(1, driver.getPIN());
                preparedStatement.setDouble(2, driver.getRating());

                preparedStatement.addBatch();
            }

            int[] result = preparedStatement.executeBatch();
            for (int count : result) {
                rowsInserted += (count >= 0) ? 1 : 0;
            }

            preparedStatement.close();
        } catch (SQLException e) {
            e.printStackTrace();
        }

        return rowsInserted;
    }

    
    //5.2 Task 2 Insert Cars
    @Override
    public int insertCars(Car[] cars) {
        int rowsInserted = 0;
        String query = "INSERT INTO Cars (CarID, PIN, color, brand) VALUES (?, ?, ?, ?)";

        try {
            PreparedStatement preparedStatement = connection.prepareStatement(query);

            for (Car car : cars) {
                preparedStatement.setInt(1, car.getCarID());
                preparedStatement.setInt(2, car.getPIN());
                preparedStatement.setString(3, car.getColor());
                preparedStatement.setString(4, car.getBrand());

                preparedStatement.addBatch();
            }

            int[] result = preparedStatement.executeBatch();
            for (int count : result) {
                rowsInserted += (count >= 0) ? 1 : 0;
            }

            preparedStatement.close();
        } catch (SQLException e) {
            e.printStackTrace();
        }

        return rowsInserted;
    }


    //5.2 Task 2 Insert Trips
    @Override
    public int insertTrips(Trip[] trips) {
        int rowsInserted = 0;
        String query = "INSERT INTO Trips (TripID, CarID, date, departure, destination, num_seats_available) VALUES (?, ?, ?, ?, ?, ?)";

        try {
            PreparedStatement preparedStatement = connection.prepareStatement(query);

            for (Trip trip : trips) {
                preparedStatement.setInt(1, trip.getTripID());
                preparedStatement.setInt(2, trip.getCarID());
                preparedStatement.setString(3, trip.getDate());
                preparedStatement.setString(4, trip.getDeparture());
                preparedStatement.setString(5, trip.getDestination());
                preparedStatement.setInt(6, trip.getNum_seats_available());

                preparedStatement.addBatch();
            }

            int[] result = preparedStatement.executeBatch();
            for (int count : result) {
                rowsInserted += (count >= 0) ? 1 : 0;
            }

            preparedStatement.close();
        } catch (SQLException e) {
            e.printStackTrace();
        }

        return rowsInserted;
    }

    //5.2 Task 2 Insert Bookings
    @Override
    public int insertBookings(Booking[] bookings) {
        int rowsInserted = 0;
        String query = "INSERT INTO Bookings (TripID, PIN, booking_status) VALUES (?, ?, ?)";

        try {
            PreparedStatement preparedStatement = connection.prepareStatement(query);

            for (Booking booking : bookings) {
                preparedStatement.setInt(1, booking.getTripID());
                preparedStatement.setInt(2, booking.getPIN());
                preparedStatement.setString(3, booking.getBooking_status());

                preparedStatement.addBatch();
            }

            int[] result = preparedStatement.executeBatch();
            for (int count : result) {
                rowsInserted += (count >= 0) ? 1 : 0;
            }

            preparedStatement.close();
        } catch (SQLException e) {
            e.printStackTrace();
        }

        return rowsInserted;
    }



    //5.3 Task 3 Find all participants who are recorded as both drivers and passengers
    @Override
    public Participant[] getBothPassengersAndDrivers() {
        List<Participant> bothParticipants = new ArrayList<>();
        String query = "SELECT P.PIN, P.p_name, P.age " +
                "FROM Participants P " +
                "JOIN Passengers Pa ON P.PIN = Pa.PIN " +
                "JOIN Drivers D ON P.PIN = D.PIN " +
                "ORDER BY P.PIN ASC";

        try {
            Statement statement = connection.createStatement();
            ResultSet resultSet = statement.executeQuery(query);

            while (resultSet.next()) {
                int pin = resultSet.getInt("PIN");
                String name = resultSet.getString("p_name");
                int age = resultSet.getInt("age");

                bothParticipants.add(new Participant(pin, name, age));
            }

            resultSet.close();
            statement.close();
        } catch (SQLException e) {
            e.printStackTrace();
        }

        return bothParticipants.toArray(new Participant[0]);
    }


 
    //5.4 Task 4 Find the PINs, names, ages, and ratings of drivers who do not own any cars
    @Override
    public QueryResult.DriverPINNameAgeRating[] getDriversWithNoCars() {
        List<QueryResult.DriverPINNameAgeRating> driversWithNoCars = new ArrayList<>();
        String query = "SELECT D.PIN, P.p_name, P.age, D.rating " +
                "FROM Drivers D " +
                "JOIN Participants P ON D.PIN = P.PIN " +
                "LEFT JOIN Cars C ON D.PIN = C.PIN " +
                "WHERE C.CarID IS NULL " +
                "ORDER BY D.PIN ASC";

        try {
            Statement statement = connection.createStatement();
            ResultSet resultSet = statement.executeQuery(query);

            while (resultSet.next()) {
                int pin = resultSet.getInt("PIN");
                String name = resultSet.getString("p_name");
                int age = resultSet.getInt("age");
                double rating = resultSet.getDouble("rating");

                driversWithNoCars.add(new QueryResult.DriverPINNameAgeRating(pin, name, age, rating));
            }

            resultSet.close();
            statement.close();
        } catch (SQLException e) {
            e.printStackTrace();
        }

        return driversWithNoCars.toArray(new QueryResult.DriverPINNameAgeRating[0]);
    }


 
    
    //5.5 Task 5 Delete Drivers who do not own any cars
    @Override
    public int deleteDriversWithNoCars() {
        int rowsDeleted = 0;
        String query = "DELETE FROM Drivers " +
                "WHERE PIN NOT IN (SELECT PIN FROM Cars)";

        try {
            Statement statement = connection.createStatement();
            rowsDeleted = statement.executeUpdate(query);
            statement.close();
        } catch (SQLException e) {
            e.printStackTrace();
        }

        return rowsDeleted;
    }


    
    //5.6 Task 6 Find all cars that are not taken part in any trips
    @Override
    public Car[] getCarsWithNoTrips() {
        List<Car> carsWithNoTrips = new ArrayList<>();
        String query = "SELECT C.CarID, C.PIN, C.color, C.brand " +
                "FROM Cars C " +
                "LEFT JOIN Trips T ON C.CarID = T.CarID " +
                "WHERE T.TripID IS NULL " +
                "ORDER BY C.CarID ASC";

        try {
            Statement statement = connection.createStatement();
            ResultSet resultSet = statement.executeQuery(query);

            while (resultSet.next()) {
                int carID = resultSet.getInt("CarID");
                int pin = resultSet.getInt("PIN");
                String color = resultSet.getString("color");
                String brand = resultSet.getString("brand");

                carsWithNoTrips.add(new Car(carID, pin, color, brand));
            }

            resultSet.close();
            statement.close();
        } catch (SQLException e) {
            e.printStackTrace();
        }

        return carsWithNoTrips.toArray(new Car[0]);
    }

    
    
    //5.7 Task 7 Find all passengers who didn't book any trips
    @Override
    public Passenger[] getPassengersWithNoBooks() {
        List<Passenger> passengersWithNoBooks = new ArrayList<>();
        String query = "SELECT P.PIN, Pa.membership_status " +
                "FROM Passengers Pa " +
                "JOIN Participants P ON Pa.PIN = P.PIN " +
                "LEFT JOIN Bookings B ON Pa.PIN = B.PIN " +
                "WHERE B.TripID IS NULL " +
                "ORDER BY P.PIN ASC";

        try {
            Statement statement = connection.createStatement();
            ResultSet resultSet = statement.executeQuery(query);

            while (resultSet.next()) {
                int pin = resultSet.getInt("PIN");
                String membershipStatus = resultSet.getString("membership_status");

                passengersWithNoBooks.add(new Passenger(pin, membershipStatus));
            }

            resultSet.close();
            statement.close();
        } catch (SQLException e) {
            e.printStackTrace();
        }

        return passengersWithNoBooks.toArray(new Passenger[0]);
    }






    //5.8 Task 8 Find all trips that depart from the specified city to specified destination city on specific date
    @Override
    public Trip[] getTripsFromToCitiesOnSpecificDate(String departure, String destination, String date) {
        List<Trip> trips = new ArrayList<>();
        String query = "SELECT TripID, CarID, date, departure, destination, num_seats_available " +
                "FROM Trips " +
                "WHERE departure = ? AND destination = ? AND date = ? " +
                "ORDER BY TripID ASC";

        try {
            PreparedStatement preparedStatement = connection.prepareStatement(query);
            preparedStatement.setString(1, departure);
            preparedStatement.setString(2, destination);
            preparedStatement.setString(3, date);

            ResultSet resultSet = preparedStatement.executeQuery();

            while (resultSet.next()) {
                int tripID = resultSet.getInt("TripID");
                int carID = resultSet.getInt("CarID");
                String tripDate = resultSet.getString("date");
                String tripDeparture = resultSet.getString("departure");
                String tripDestination = resultSet.getString("destination");
                int numSeatsAvailable = resultSet.getInt("num_seats_available");

                trips.add(new Trip(tripID, carID, tripDate, tripDeparture, tripDestination, numSeatsAvailable));
            }

            resultSet.close();
            preparedStatement.close();
        } catch (SQLException e) {
            e.printStackTrace();
        }

        return trips.toArray(new Trip[0]);
    }




    //5.9 Task 9 Find the PINs, names, ages, and membership_status of passengers who have bookings on all trips destined at a particular city
    @Override
    public QueryResult.PassengerPINNameAgeMembershipStatus[] getPassengersWithBookingsToAllTripsForCity(String city) {
        List<QueryResult.PassengerPINNameAgeMembershipStatus> passengers = new ArrayList<>();

        String query = "SELECT P.PIN, P.p_name, P.age, Pa.membership_status " +
                "FROM Passengers Pa " +
                "JOIN Participants P ON Pa.PIN = P.PIN " +
                "JOIN Bookings B ON Pa.PIN = B.PIN " +
                "JOIN Trips T ON B.TripID = T.TripID " +
                "WHERE T.destination = ? " +
                "GROUP BY P.PIN, P.p_name, P.age, Pa.membership_status " +
                "HAVING COUNT(DISTINCT T.TripID) = (SELECT COUNT(*) FROM Trips WHERE destination = ?) " +
                "ORDER BY P.PIN ASC";

        try {
            PreparedStatement preparedStatement = connection.prepareStatement(query);
            preparedStatement.setString(1, city);
            preparedStatement.setString(2, city);

            ResultSet resultSet = preparedStatement.executeQuery();

            while (resultSet.next()) {
                int pin = resultSet.getInt("PIN");
                String name = resultSet.getString("p_name");
                int age = resultSet.getInt("age");
                String membershipStatus = resultSet.getString("membership_status");

                passengers.add(new QueryResult.PassengerPINNameAgeMembershipStatus(pin, name, age, membershipStatus));
            }

            resultSet.close();
            preparedStatement.close();
        } catch (SQLException e) {
            e.printStackTrace();
        }

        return passengers.toArray(new QueryResult.PassengerPINNameAgeMembershipStatus[0]);
    }



    
    //5.10 Task 10 For a given driver PIN, find the CarIDs that the driver owns and were booked at most twice.    
    @Override
    public Integer[] getDriverCarsWithAtMost2Bookings(int driverPIN) {
        List<Integer> carIDs = new ArrayList<>();

        // SQL query: Find cars owned by the driver that have at most 2 bookings.
        String query = "SELECT C.CarID " +
                "FROM Cars C " +
                "JOIN Trips T ON C.CarID = T.CarID " +
                "JOIN Bookings B ON T.TripID = B.TripID " +
                "WHERE C.PIN = ? " +
                "GROUP BY C.CarID " +
                "HAVING COUNT(B.TripID) <= 2 " +
                "ORDER BY C.CarID ASC";

        try (PreparedStatement preparedStatement = connection.prepareStatement(query)) {
            // Set the driver's PIN as a parameter in the query
            preparedStatement.setInt(1, driverPIN);

            // Execute the query and get the result set
            try (ResultSet resultSet = preparedStatement.executeQuery()) {
                // Loop through the results and add the CarID to the list
                while (resultSet.next()) {
                    int carID = resultSet.getInt("CarID");
                    carIDs.add(carID);
                }
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }

        // Convert the list to an array and return it
        return carIDs.toArray(new Integer[0]);
    }



    //5.11 Task 11 Find the average age of passengers with "Confirmed" bookings (i.e., booking_status is ”Confirmed”) on trips departing from a given city and within a specified date range
    @Override
    public Double getAvgAgeOfPassengersDepartFromCityBetweenTwoDates(String city, String start_date, String end_date) {
        Double averageAge = null;

        // SQL query to get the average age of passengers with confirmed bookings
        String query = "SELECT AVG(P.age) AS avg_age " +
                "FROM Passengers Pa " +
                "JOIN Participants P ON Pa.PIN = P.PIN " +
                "JOIN Bookings B ON Pa.PIN = B.PIN " +
                "JOIN Trips T ON B.TripID = T.TripID " +
                "WHERE T.departure = ? " +  // Filter by the specified city
                "AND T.date BETWEEN ? AND ? " +  // Filter by the date range
                "AND B.booking_status = 'Confirmed'";  // Filter by confirmed bookings

        try (PreparedStatement preparedStatement = connection.prepareStatement(query)) {
            // Set the query parameters: city, start_date, end_date
            preparedStatement.setString(1, city);
            preparedStatement.setString(2, start_date);
            preparedStatement.setString(3, end_date);

            // Execute the query and get the result set
            try (ResultSet resultSet = preparedStatement.executeQuery()) {
                if (resultSet.next()) {
                    // Get the average age from the result set
                    averageAge = resultSet.getDouble("avg_age");
                }
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }

        return averageAge;
    }


    //5.12 Task 12 Find Passengers in a Given Trip.
    @Override
    public QueryResult.PassengerPINNameAgeMembershipStatus[] getPassengerInGivenTrip(int TripID) {
        List<QueryResult.PassengerPINNameAgeMembershipStatus> passengers = new ArrayList<>();

        // SQL query to get passengers who have booked the specified trip
        String query = "SELECT P.PIN, P.p_name, P.age, Pa.membership_status " +
                "FROM Passengers Pa " +
                "JOIN Participants P ON Pa.PIN = P.PIN " +
                "JOIN Bookings B ON Pa.PIN = B.PIN " +
                "WHERE B.TripID = ? " +  // Filter by the specified TripID
                "ORDER BY P.PIN ASC";  // Sort by PIN in ascending order

        try (PreparedStatement preparedStatement = connection.prepareStatement(query)) {
            // Set the TripID parameter
            preparedStatement.setInt(1, TripID);

            // Execute the query and get the result set
            try (ResultSet resultSet = preparedStatement.executeQuery()) {
                while (resultSet.next()) {
                    // Get the passenger details from the result set
                    int pin = resultSet.getInt("PIN");
                    String name = resultSet.getString("p_name");
                    int age = resultSet.getInt("age");
                    String membershipStatus = resultSet.getString("membership_status");

                    // Add the passenger to the list
                    passengers.add(new QueryResult.PassengerPINNameAgeMembershipStatus(pin, name, age, membershipStatus));
                }
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }

        // Return the result as an array
        return passengers.toArray(new QueryResult.PassengerPINNameAgeMembershipStatus[0]);
    }




    //5.13 Task 13 Find Drivers’ Scores
    @Override
    public QueryResult.DriverScoreRatingNumberOfBookingsPIN[] getDriversScores() {
        List<QueryResult.DriverScoreRatingNumberOfBookingsPIN> driverScores = new ArrayList<>();

        String query = "SELECT D.PIN AS DriverPIN, D.rating, COUNT(B.TripID) AS numberOfBookings, " +
                "(D.rating * COUNT(B.TripID)) AS driverScore " +
                "FROM Drivers D " +
                "JOIN Cars C ON D.PIN = C.PIN " +
                "JOIN Trips T ON C.CarID = T.CarID " +
                "JOIN Bookings B ON T.TripID = B.TripID " +
                "GROUP BY D.PIN, D.rating " +
                "ORDER BY driverScore DESC, D.PIN ASC";

        try (PreparedStatement preparedStatement = connection.prepareStatement(query);
             ResultSet resultSet = preparedStatement.executeQuery()) {

            while (resultSet.next()) {
                int driverPIN = resultSet.getInt("DriverPIN");
                double rating = resultSet.getDouble("rating");
                int numberOfBookings = resultSet.getInt("numberOfBookings");
                double driverScore = resultSet.getDouble("driverScore");

                driverScores.add(new QueryResult.DriverScoreRatingNumberOfBookingsPIN(driverScore, rating, numberOfBookings, driverPIN));
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }

        // Return the result as an array
        return driverScores.toArray(new QueryResult.DriverScoreRatingNumberOfBookingsPIN[0]);
    }



    
    //5.14 Task 14 Find average ratings of drivers who have trips destined to each city
    @Override
    public QueryResult.CityAndAverageDriverRating[] getDriversAverageRatingsToEachDestinatedCity() {
        List<QueryResult.CityAndAverageDriverRating> cityAverageRatings = new ArrayList<>();

        String query = "SELECT T.destination AS city, AVG(D.rating) AS averageDriverRating " +
                "FROM Drivers D " +
                "JOIN Cars C ON D.PIN = C.PIN " +
                "JOIN Trips T ON C.CarID = T.CarID " +
                "GROUP BY T.destination " +
                "ORDER BY T.destination ASC";

        try (PreparedStatement preparedStatement = connection.prepareStatement(query);
             ResultSet resultSet = preparedStatement.executeQuery()) {

            while (resultSet.next()) {
                String city = resultSet.getString("city");
                double averageDriverRating = resultSet.getDouble("averageDriverRating");

                cityAverageRatings.add(new QueryResult.CityAndAverageDriverRating(city, averageDriverRating));
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }

        // Return the result as an array
        return cityAverageRatings.toArray(new QueryResult.CityAndAverageDriverRating[0]);
    }




    //5.15 Task 15 Find total number of bookings of passengers for each membership status
    @Override
    public QueryResult.MembershipStatusAndTotalBookings[] getTotalBookingsEachMembershipStatus() {
        List<QueryResult.MembershipStatusAndTotalBookings> results = new ArrayList<>();

        String query = "SELECT Pa.membership_status, COUNT(B.TripID) AS total_bookings " +
                "FROM Passengers Pa " +
                "JOIN Bookings B ON Pa.PIN = B.PIN " +
                "GROUP BY Pa.membership_status " +
                "ORDER BY Pa.membership_status ASC";

        try {
            Statement statement = connection.createStatement();
            ResultSet resultSet = statement.executeQuery(query);

            while (resultSet.next()) {
                String membershipStatus = resultSet.getString("membership_status");
                int totalBookings = resultSet.getInt("total_bookings");

                // Add the result to the list
                results.add(new QueryResult.MembershipStatusAndTotalBookings(membershipStatus, totalBookings));
            }

            resultSet.close();
            statement.close();

        } catch (SQLException e) {
            e.printStackTrace();
        }

        // Return the list as an array
        return results.toArray(new QueryResult.MembershipStatusAndTotalBookings[0]);
    }



    
    //5.16 Task 16 For the drivers' ratings, if rating is smaller than 2.0 or equal to 2.0, update the rating by adding 0.5.
    @Override
    public int updateDriverRatings() {
        int rowsUpdated = 0;

        // SQL query to update ratings for drivers whose rating is less than or equal to 2.0
        String query = "UPDATE Drivers SET rating = rating + 0.5 WHERE rating <= 2.0";

        try {
            // Execute the update query
            Statement statement = connection.createStatement();
            rowsUpdated = statement.executeUpdate(query);
            statement.close();
        } catch (SQLException e) {
            e.printStackTrace();
        }

        return rowsUpdated;
    }





    //6.1 (Optional) Task 18 Find trips departing from the given city
    @Override
    public Trip[] getTripsFromCity(String city) {
        
    	/*****************************************************/
        /*****************************************************/
        /*****************  TODO  (Optional)   ***************/
        /*****************************************************/
        /*****************************************************/
    	
        return new Trip[0];
    }
    
    
    //6.2 (Optional) Task 19 Find all trips that have never been booked
    @Override
    public Trip[] getTripsWithNoBooks() {
        
    	/*****************************************************/
        /*****************************************************/
        /*****************  TODO  (Optional)   ***************/
        /*****************************************************/
        /*****************************************************/
    	
        return new Trip[0];
    }
    
    
    //6.3 (Optional) Task 20 For each driver, find the trip(s) with the highest number of bookings
    @Override
    public QueryResult.DriverPINandTripIDandNumberOfBookings[] getTheMostBookedTripsPerDriver() {
        
    	/*****************************************************/
        /*****************************************************/
        /*****************  TODO  (Optional)   ***************/
        /*****************************************************/
        /*****************************************************/
    	
        return new QueryResult.DriverPINandTripIDandNumberOfBookings[0];
    }
    
    
    //6.4 (Optional) Task 21 Find Full Cars
    @Override
    public QueryResult.FullCars[] getFullCars() {
        
    	/*****************************************************/
        /*****************************************************/
        /*****************  TODO  (Optional)   ***************/
        /*****************************************************/
        /*****************************************************/
    	
        return new QueryResult.FullCars[0];
    }

}
