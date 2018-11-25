// Copyright (c) Philipp Wagner. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package de.bytefish.pgbulkinsert.pgsql.handlers;

import de.bytefish.pgbulkinsert.PgBulkInsert;
import de.bytefish.pgbulkinsert.mapping.AbstractMapping;
import de.bytefish.pgbulkinsert.pgsql.handlers.utils.GeometricUtils;
import de.bytefish.pgbulkinsert.pgsql.model.geometric.*;
import de.bytefish.pgbulkinsert.util.JavaUtils;
import de.bytefish.pgbulkinsert.util.PostgreSqlUtils;
import de.bytefish.pgbulkinsert.utils.TransactionalTestBase;
import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;
import org.postgresql.geometric.*;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class GeometricTypesTest extends TransactionalTestBase {

    private class GeometricEntity {

        private Point col_point;
        private Path col_path;
        private Polygon col_polygon;
        private Box col_box;
        private Line col_line;
        private Circle col_circle;
        private LineSegment col_line_segment;

        public void setCol_point(Point col_point) {
            this.col_point = col_point;
        }

        public void setCol_path(Path col_path) {
            this.col_path = col_path;
        }

        public void setCol_polygon(Polygon col_polygon) {
            this.col_polygon = col_polygon;
        }

        public void setCol_box(Box col_box) {
            this.col_box = col_box;
        }

        public void setCol_line(Line col_line) {
            this.col_line = col_line;
        }

        public void setCol_circle(Circle col_circle) {
            this.col_circle = col_circle;
        }

        public void setCol_line_segment(LineSegment col_line_segment) {
            this.col_line_segment = col_line_segment;
        }

        public Point getCol_point() {
            return col_point;
        }

        public Path getCol_path() {
            return col_path;
        }

        public Polygon getCol_polygon() {
            return col_polygon;
        }

        public Box getCol_box() {
            return col_box;
        }

        public Line getCol_line() {
            return col_line;
        }

        public Circle getCol_circle() {
            return col_circle;
        }

        public LineSegment getCol_line_segment() {
            return col_line_segment;
        }
    }

    @Override
    protected void onSetUpInTransaction() throws Exception {
        createTable();
    }

    @Override
    protected void onSetUpBeforeTransaction() throws Exception {

    }

    private class GeometricEntityMapping extends AbstractMapping<GeometricEntity> {

        public GeometricEntityMapping() {
            super(SCHEMA, "geometric_table");

            mapPoint("col_point", GeometricEntity::getCol_point);
            mapPath("col_path", GeometricEntity::getCol_path);
            mapPolygon("col_polygon", GeometricEntity::getCol_polygon);
            mapBox("col_box", GeometricEntity::getCol_box);
            mapLine("col_line", GeometricEntity::getCol_line);
            mapCircle("col_circle", GeometricEntity::getCol_circle);
            mapLineSegment("col_line_segment", GeometricEntity::getCol_line_segment);
        }
    }

    private boolean createTable() throws SQLException {
        String sqlStatement = String.format("CREATE TABLE %s.geometric_table(\n", SCHEMA) +
                "                col_point point, \n" +
                "                col_path path, \n" +
                "                col_polygon polygon, \n" +
                "                col_box box, \n" +
                "                col_line line, \n" +
                "                col_circle circle, \n" +
                "                col_line_segment lseg \n" +
                "            );";

        Statement statement = connection.createStatement();

        return statement.execute(sqlStatement);
    }


    @Test
    public void saveAll_Point_Test() throws SQLException {

        // This list will be inserted.
        List<GeometricEntity> entities = new ArrayList<>();

        // Point to insert:
        Point p = new Point(1.0, 2.0);

        // Build the Entities to store:
        GeometricEntity entity = new GeometricEntity();

        entity.setCol_point(p);

        entities.add(entity);

        // Construct the Insert:
        PgBulkInsert<GeometricEntity> bulkInsert = new PgBulkInsert<>(new GeometricEntityMapping());

        // Save them:
        bulkInsert.saveAll(PostgreSqlUtils.getPGConnection(connection), entities.stream());

        ResultSet rs = getAll();

        while (rs.next()) {
            PGpoint v = (PGpoint) rs.getObject("col_point");

            Assert.assertEquals(1.0, v.x, 1e-10);
            Assert.assertEquals(2.0, v.y, 1e-10);


            Assert.assertNotNull(v);
        }
    }

    @Test
    public void saveAll_Path_Test() throws SQLException {

        // This list will be inserted.
        List<GeometricEntity> entities = new ArrayList<>();

        // Points on the Path:
        List<Point> points = new ArrayList<>();

        points.add(new Point(1.0, 2.0));
        points.add(new Point(3.0, 4.0));

        // Point to insert:
        Path p = new Path(false, points);

        // Build the Entities to store:
        GeometricEntity entity = new GeometricEntity();

        entity.setCol_path(p);

        entities.add(entity);

        // Construct the Insert:
        PgBulkInsert<GeometricEntity> bulkInsert = new PgBulkInsert<>(new GeometricEntityMapping());

        bulkInsert.saveAll(PostgreSqlUtils.getPGConnection(connection), entities.stream());

        ResultSet rs = getAll();

        while (rs.next()) {
            PGpath v = (PGpath) rs.getObject("col_path");

            Assert.assertNotNull(v);

            Assert.assertEquals(false, v.isClosed());
            Assert.assertEquals(2, v.points.length);

            Assert.assertEquals(1.0, v.points[0].x, 1e-10);
            Assert.assertEquals(2.0, v.points[0].y, 1e-10);

            Assert.assertEquals(3.0, v.points[1].x, 1e-10);
            Assert.assertEquals(4.0, v.points[1].y, 1e-10);

        }
    }

    @Test
    public void saveAll_Polygon_Test() throws SQLException {

        // This list will be inserted.
        List<GeometricEntity> entities = new ArrayList<>();

        // Points on the Path:
        List<Point> points = new ArrayList<>();

        points.add(new Point(1.0, 2.0));
        points.add(new Point(3.0, 4.0));

        // Point to insert:
        Polygon p = new Polygon(points);

        // Build the Entities to store:
        GeometricEntity entity = new GeometricEntity();

        entity.setCol_polygon(p);

        entities.add(entity);

        // Construct the Insert:
        PgBulkInsert<GeometricEntity> bulkInsert = new PgBulkInsert<>(new GeometricEntityMapping());

        bulkInsert.saveAll(PostgreSqlUtils.getPGConnection(connection), entities.stream());

        ResultSet rs = getAll();

        while (rs.next()) {
            PGpolygon v = (PGpolygon) rs.getObject("col_polygon");

            Assert.assertNotNull(v);

            Assert.assertEquals(2, v.points.length);

            Assert.assertEquals(1.0, v.points[0].x, 1e-10);
            Assert.assertEquals(2.0, v.points[0].y, 1e-10);

            Assert.assertEquals(3.0, v.points[1].x, 1e-10);
            Assert.assertEquals(4.0, v.points[1].y, 1e-10);

        }
    }

    @Test
    public void saveAll_Line_Test() throws SQLException {

        // This list will be inserted.
        List<GeometricEntity> entities = new ArrayList<>();

        // Point to insert:
        Line line = new Line(1, 2, 3);

        // Build the Entities to store:
        GeometricEntity entity = new GeometricEntity();

        entity.setCol_line(line);

        entities.add(entity);

        // Construct the Insert:
        PgBulkInsert<GeometricEntity> bulkInsert = new PgBulkInsert<>(new GeometricEntityMapping());

        bulkInsert.saveAll(PostgreSqlUtils.getPGConnection(connection), entities.stream());

        ResultSet rs = getAll();

        while (rs.next()) {
            PGline v = (PGline) rs.getObject("col_line");

            Assert.assertNotNull(v);

            Assert.assertEquals(1, v.a, 1e-10);
            Assert.assertEquals(2, v.b, 1e-10);
            Assert.assertEquals(3, v.c, 1e-10);
        }
    }

    @Test
    public void saveAll_Line_Segment_Test() throws SQLException {

        // This list will be inserted.
        List<GeometricEntity> entities = new ArrayList<>();

        Point p1 = new Point(1.0, 2.0);
        Point p2 = new Point(3.0, 4.0);

        // Point to insert:
        LineSegment line = new LineSegment(p1, p2);

        // Build the Entities to store:
        GeometricEntity entity = new GeometricEntity();

        entity.setCol_line_segment(line);

        entities.add(entity);

        // Construct the Insert:
        PgBulkInsert<GeometricEntity> bulkInsert = new PgBulkInsert<>(new GeometricEntityMapping());

        bulkInsert.saveAll(PostgreSqlUtils.getPGConnection(connection), entities.stream());

        ResultSet rs = getAll();

        while (rs.next()) {
            PGlseg v = (PGlseg) rs.getObject("col_line_segment");

            Assert.assertNotNull(v);

            Assert.assertEquals(1.0, v.point[0].x, 1e-10);
            Assert.assertEquals(2.0, v.point[0].y, 1e-10);

            Assert.assertEquals(3.0, v.point[1].x, 1e-10);
            Assert.assertEquals(4.0, v.point[1].y, 1e-10);
        }
    }

    @Test
    public void saveAll_Box_Test() throws SQLException {

        // This list will be inserted.
        List<GeometricEntity> entities = new ArrayList<>();

        Point p1 = new Point(1.0, 2.0);
        Point p2 = new Point(3.0, 4.0);

        // Point to insert:
        Box box = new Box(p1, p2);

        // Build the Entities to store:
        GeometricEntity entity = new GeometricEntity();

        entity.setCol_box(box);

        entities.add(entity);

        // Construct the Insert:
        PgBulkInsert<GeometricEntity> bulkInsert = new PgBulkInsert<>(new GeometricEntityMapping());

        bulkInsert.saveAll(PostgreSqlUtils.getPGConnection(connection), entities.stream());

        ResultSet rs = getAll();

        while (rs.next()) {
            PGbox v = (PGbox) rs.getObject("col_box");

            Assert.assertNotNull(v);

            Assert.assertEquals(3.0, v.point[0].x, 1e-10);
            Assert.assertEquals(4.0, v.point[0].y, 1e-10);

            Assert.assertEquals(1.0, v.point[1].x, 1e-10);
            Assert.assertEquals(2.0, v.point[1].y, 1e-10);
        }
    }


    @Test
    public void saveAll_Circle_Test() throws SQLException {

        // This list will be inserted.
        List<GeometricEntity> entities = new ArrayList<>();

        Point center = new Point(1.0, 2.0);
        double radius = 4.1;

        // Point to insert:
        Circle circle = new Circle(center, radius);

        // Build the Entities to store:
        GeometricEntity entity = new GeometricEntity();

        entity.setCol_circle(circle);

        entities.add(entity);

        // Construct the Insert:
        PgBulkInsert<GeometricEntity> bulkInsert = new PgBulkInsert<>(new GeometricEntityMapping());

        // Save them:
        bulkInsert.saveAll(PostgreSqlUtils.getPGConnection(connection), entities.stream());

        ResultSet rs = getAll();

        while (rs.next()) {
            PGcircle v = (PGcircle) rs.getObject("col_circle");

            Assert.assertNotNull(v);

            Assert.assertEquals(1.0, v.center.x, 1e-10);
            Assert.assertEquals(2.0, v.center.y, 1e-10);

            Assert.assertEquals(4.1, v.radius, 1e-10);
        }
    }


    private ResultSet getAll() throws SQLException {
        String sqlStatement = String.format("SELECT * FROM %s.geometric_table", SCHEMA);

        Statement statement = connection.createStatement();

        return statement.executeQuery(sqlStatement);
    }



}
