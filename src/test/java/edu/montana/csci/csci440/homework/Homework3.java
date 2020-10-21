package edu.montana.csci.csci440.homework;

import edu.montana.csci.csci440.DBTest;
import org.junit.jupiter.api.Test;
import java.util.List;
import java.util.Map;
import static org.junit.jupiter.api.Assertions.assertEquals;
public class Homework3 extends DBTest {

    @Test
    /*
     * Done
     */
    public void createTracksPlusView(){
        //TODO fill this in
        executeDDL("CREATE VIEW tracksPlus AS " +
                "SELECT tracks.*, " +
                "albums.Title as AlbumTitle, " +
                "artists.Name as ArtistName, " +
                "genres.Name as GenreName " +
                "FROM tracks " +
                "JOIN albums ON " +
                "tracks.AlbumId = albums.AlbumId " +
                "JOIN artists ON " +
                "albums.ArtistId = artists.ArtistId " +
                "JOIN genres ON " +
                "tracks.GenreId = genres.GenreID"
        );

        List<Map<String, Object>> results = executeSQL("SELECT * FROM tracksPlus ORDER BY TrackId");
        assertEquals(3503, results.size());
        assertEquals("Rock", results.get(0).get("GenreName"));
        assertEquals("AC/DC", results.get(0).get("ArtistName"));
        assertEquals("For Those About To Rock We Salute You", results.get(0).get("AlbumTitle"));
    }

    @Test
    /*
     * Done
     */
    public void createGrammyInfoTable(){
        //TODO fill these in
        executeDDL("CREATE TABLE grammy_categories(" +
                "GrammyCategoryId INTEGER NOT NULL PRIMARY KEY," +
                "Name TEXT);");

        executeDDL("CREATE TABLE grammy_infos(" +
                "ArtistId INTEGER, " +
                "AlbumId INTEGER, " +
                "TrackId INTEGER, " +
                "GrammyCategoryId INTEGER, " +
                "Status TEXT," +
                "FOREIGN KEY (GrammyCategoryId) " +
                "REFERENCES grammy_categories (GrammyCategoryId));");

        // TEST CODE
        executeUpdate("INSERT INTO grammy_categories(Name) VALUES ('Greatest Ever');");
        Object categoryId = executeSQL("SELECT GrammyCategoryId FROM grammy_categories").get(0).get("GrammyCategoryId");

        executeUpdate("INSERT INTO grammy_infos(ArtistId, AlbumId, TrackId, GrammyCategoryId, Status) VALUES (1, 1, 1, " + categoryId + ",'Won');");

        List<Map<String, Object>> results = executeSQL("SELECT * FROM grammy_infos");
        assertEquals(1, results.size());
        assertEquals(1, results.get(0).get("ArtistId"));
        assertEquals(1, results.get(0).get("AlbumId"));
        assertEquals(1, results.get(0).get("TrackId"));
        assertEquals(1, results.get(0).get("GrammyCategoryId"));
    }

    @Test
    /*
     * Done
     */
    public void bulkInsertGenres(){
        Integer before = (Integer) executeSQL("SELECT COUNT(*) as COUNT FROM genres").get(0).get("COUNT");

        //TODO fill this in
        executeUpdate("INSERT INTO genres (Name) VALUES ('Vaporwave'), ('JazzFusion'), ('Synthwave'), ('FolkMetal'), ('MusicforChickens')");



        Integer after = (Integer) executeSQL("SELECT COUNT(*) as COUNT FROM genres").get(0).get("COUNT");
        assertEquals(before + 5, after);
    }

}
