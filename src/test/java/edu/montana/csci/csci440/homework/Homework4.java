package edu.montana.csci.csci440.homework;

import edu.montana.csci.csci440.DBTest;
import edu.montana.csci.csci440.model.Track;
import edu.montana.csci.csci440.util.DB;
import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class Homework4 extends DBTest {

    @Test
    /*
     * Use a transaction to safely move milliseconds from one track to anotherls
     *
     * You will need to use the JDBC transaction API, outlined here:
     *
     *   https://docs.oracle.com/javase/tutorial/jdbc/basics/transactions.html
     *
     */
    public void useATransactionToSafelyMoveMillisecondsFromOneTrackToAnother() throws SQLException {

        Track track1 = Track.find(1);
        Long track1InitialTime = track1.getMilliseconds();
        Track track2 = Track.find(2);
        Long track2InitialTime = track2.getMilliseconds();

        try(Connection connection = DB.connect()){
            connection.setAutoCommit(false);
            PreparedStatement subtract = connection.prepareStatement("update tracks set Milliseconds = ? where Milliseconds = ?");
            subtract.setLong(1, 0);
            subtract.setLong(2, 0);
            subtract.execute();

            PreparedStatement add = connection.prepareStatement("update tracks set Milliseconds = ? where Milliseconds = ?");
            subtract.setLong(1, 0);
            subtract.setLong(2, 0);
            subtract.execute();

            connection.commit();
        }

        // refresh tracks from db
        track1 = Track.find(1);
        track2 = Track.find(2);
        assertEquals(track1.getMilliseconds(), track1InitialTime - 0);
        assertEquals(track2.getMilliseconds(), track2InitialTime + 0);
    }

    @Test
    /*
     * Select tracks that have been sold more than once (> 1)
     *
     * Select the albums that have tracks that have been sold more than once (> 1)
     *   NOTE: This is NOT the same as albums whose tracks have been sold more than once!
     *         An album could have had three tracks, each sold once, and should not be included
     *         in this result.  It should only include the albums of the tracks found in the first
     *         query.
     * */
    public void selectPopularTracksAndTheirAlbums() throws SQLException {

        // HINT: join to invoice items and do a group by/having to get the right answer
        List<Map<String, Object>> tracks = executeSQL("SELECT tracks.name " +
                "FROM tracks " +
                "LEFT JOIN invoice_items " +
                "ON tracks.TrackID = invoice_items.TrackID " +
                "GROUP BY invoice_items.TrackID " +
                "HAVING COUNT (*) > 1");
        assertEquals(257, tracks.size());

        // HINT: join to tracks and invoice items and do a group by/having to get the right answer
        //       note: you will need to use the DISTINCT operator to get the right result!
        List<Map<String, Object>> albums = executeSQL("SELECT albums.Title,\n" +
                "       COUNT(tracks.TrackId) as Tracks,\n" +
                "       COUNT(DISTINCT albums.AlbumId) as Albums\n" +
                "FROM tracks\n" +
                "         JOIN albums on tracks.AlbumId = albums.AlbumId\n" +
                "         JOIN invoice_items on tracks.TrackId = invoice_items.TrackId\n" +
                "GROUP BY albums.ArtistId\n" +
                "HAVING Tracks >= 1;");
        assertEquals(165, albums.size());
    }

    @Test
    /*
     * Select customers emails who are assigned to Jane Peacock as a Rep and
     * who have purchased something from the 'Rock' Genre
     *
     * Please use an IN clause and a sub-select to generate customer IDs satisfying the criteria
     * */
    public void selectCustomersMeetingCriteria() throws SQLException {
        // HINT: join to invoice items and do a group by/having to get the right answer
        List<Map<String, Object>> tracks = executeSQL("SELECT Email " +
                "FROM customers " +
                "WHERE SupportRepId IN " +
                        "(SELECT EmployeeID " +
                "FROM employees " +
                " Where employees.EmployeeID = 3)");
        assertEquals(21, tracks.size());
    }


}