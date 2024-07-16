package eu.kudljo.peopledb.repository;

import eu.kudljo.peopledb.model.Person;

import java.math.BigDecimal;
import java.sql.*;
import java.time.ZoneId;
import java.time.ZonedDateTime;

public class PeopleRepository extends CRUDRepository<Person> {
    public static final String SAVE_PERSON_SQL = "INSERT INTO PEOPLE (FIRST_NAME, LAST_NAME, DOB) VALUES (?, ?, ?)";
    private static final String FIND_PERSON_BY_ID_SQL = "SELECT ID, FIRST_NAME, LAST_NAME, DOB, SALARY FROM PEOPLE WHERE ID = ?";
    private static final String FIND_ALL_SQL = "SELECT ID, FIRST_NAME, LAST_NAME, DOB, SALARY FROM PEOPLE";
    private static final String COUNT_PEOPLE_SQL = "SELECT COUNT(*) AS COUNT FROM PEOPLE";
    private static final String DELETE_PERSON_BY_ID_SQL = "DELETE FROM PEOPLE WHERE ID = ?";
    private static final String DELETE_PEOPLE_BY_ID_SQL = "DELETE FROM PEOPLE WHERE ID IN (:ids)";
    private static final String UPDATE_PERSON_BY_ID_SQL = "UPDATE PEOPLE SET FIRST_NAME = ?, LAST_NAME = ?, DOB = ?, SALARY = ? WHERE ID = ?";

    public PeopleRepository(Connection connection) {
        super(connection);
    }

    @Override
    void mapForSave(Person person, PreparedStatement preparedStatement) throws SQLException {
        preparedStatement.setString(1, person.getFirstName());
        preparedStatement.setString(2, person.getLastName());
        preparedStatement.setTimestamp(3, convertDobToTimestamp(person.getDob()));
    }

    @Override
    void mapForUpdate(Person person, PreparedStatement preparedStatement) throws SQLException {
        preparedStatement.setString(1, person.getFirstName());
        preparedStatement.setString(2, person.getLastName());
        preparedStatement.setTimestamp(3, convertDobToTimestamp(person.getDob()));
        preparedStatement.setBigDecimal(4, person.getSalary());
        preparedStatement.setLong(5, person.getId());
    }

    @Override
    Person extractEntityFromResultSet(ResultSet resultSet) throws SQLException {
        long personId = resultSet.getLong("ID");
        String firstName = resultSet.getString("FIRST_NAME");
        String lastName = resultSet.getString("LAST_NAME");
        ZonedDateTime dob = ZonedDateTime.of(resultSet.getTimestamp("DOB").toLocalDateTime(), ZoneId.of("+0"));
        BigDecimal salary = resultSet.getBigDecimal("SALARY");
        return new Person(personId, firstName, lastName, dob, salary);
    }

    @Override
    protected String getSaveSql() {
        return SAVE_PERSON_SQL;
    }

    @Override
    protected String getFindByIdSql() {
        return FIND_PERSON_BY_ID_SQL;
    }

    @Override
    protected String getFindAllSql() {
        return FIND_ALL_SQL;
    }

    @Override
    protected String getCountSql() {
        return COUNT_PEOPLE_SQL;
    }

    @Override
    protected String getDeleteSql() {
        return DELETE_PERSON_BY_ID_SQL;
    }

    @Override
    protected String getDeleteInSql() {
        return DELETE_PEOPLE_BY_ID_SQL;
    }

    @Override
    protected String GetUpdateSql() {
        return UPDATE_PERSON_BY_ID_SQL;
    }

    private static Timestamp convertDobToTimestamp(ZonedDateTime dob) {
        return Timestamp.valueOf(dob.withZoneSameInstant(ZoneId.of("+0")).toLocalDateTime());
    }
}
