package eu.kudljo.peopledb.repository;

import eu.kudljo.peopledb.annotation.SQL;
import eu.kudljo.peopledb.model.CrudOperation;
import eu.kudljo.peopledb.model.Person;

import java.math.BigDecimal;
import java.sql.*;
import java.time.ZoneId;
import java.time.ZonedDateTime;

public class PeopleRepository extends CRUDRepository<Person> {
    public static final String SAVE_PERSON_SQL = """
            INSERT INTO PEOPLE
            (FIRST_NAME, LAST_NAME, DOB, SALARY, EMAIL)
            VALUES (?, ?, ?, ?, ?)
            """;
    private static final String FIND_PERSON_BY_ID_SQL = "SELECT ID, FIRST_NAME, LAST_NAME, DOB, SALARY FROM PEOPLE WHERE ID = ?";
    private static final String FIND_ALL_SQL = "SELECT ID, FIRST_NAME, LAST_NAME, DOB, SALARY FROM PEOPLE";
    private static final String COUNT_PEOPLE_SQL = "SELECT COUNT(*) AS COUNT FROM PEOPLE";
    private static final String DELETE_PERSON_BY_ID_SQL = "DELETE FROM PEOPLE WHERE ID = ?";
    private static final String DELETE_PEOPLE_BY_IDS_SQL = "DELETE FROM PEOPLE WHERE ID IN (:ids)";
    private static final String UPDATE_PERSON_BY_ID_SQL = "UPDATE PEOPLE SET FIRST_NAME = ?, LAST_NAME = ?, DOB = ?, SALARY = ? WHERE ID = ?";

    public PeopleRepository(Connection connection) {
        super(connection);
    }

    @Override
    @SQL(value = SAVE_PERSON_SQL, operationType = CrudOperation.SAVE)
    void mapForSave(Person person, PreparedStatement preparedStatement) throws SQLException {
        preparedStatement.setString(1, person.getFirstName());
        preparedStatement.setString(2, person.getLastName());
        preparedStatement.setTimestamp(3, convertDobToTimestamp(person.getDob()));
        preparedStatement.setBigDecimal(4, person.getSalary());
        preparedStatement.setString(5, person.getEmail());
    }

    @Override
    @SQL(value = UPDATE_PERSON_BY_ID_SQL, operationType = CrudOperation.UPDATE)
    void mapForUpdate(Person person, PreparedStatement preparedStatement) throws SQLException {
        preparedStatement.setString(1, person.getFirstName());
        preparedStatement.setString(2, person.getLastName());
        preparedStatement.setTimestamp(3, convertDobToTimestamp(person.getDob()));
        preparedStatement.setBigDecimal(4, person.getSalary());
        preparedStatement.setLong(5, person.getId());
    }

    @Override
    @SQL(value = FIND_PERSON_BY_ID_SQL, operationType = CrudOperation.FIND_BY_ID)
    @SQL(value = FIND_ALL_SQL, operationType = CrudOperation.FIND_ALL)
    @SQL(value = COUNT_PEOPLE_SQL, operationType = CrudOperation.COUNT)
    @SQL(value = DELETE_PERSON_BY_ID_SQL, operationType = CrudOperation.DELETE_BY_ID)
    @SQL(value = DELETE_PEOPLE_BY_IDS_SQL, operationType = CrudOperation.DELETE_BY_IDS)
    Person extractEntityFromResultSet(ResultSet resultSet) throws SQLException {
        long personId = resultSet.getLong("ID");
        String firstName = resultSet.getString("FIRST_NAME");
        String lastName = resultSet.getString("LAST_NAME");
        ZonedDateTime dob = ZonedDateTime.of(resultSet.getTimestamp("DOB").toLocalDateTime(), ZoneId.of("+0"));
        BigDecimal salary = resultSet.getBigDecimal("SALARY");
        return new Person(personId, firstName, lastName, dob, salary);
    }

    private static Timestamp convertDobToTimestamp(ZonedDateTime dob) {
        return Timestamp.valueOf(dob.withZoneSameInstant(ZoneId.of("+0")).toLocalDateTime());
    }
}
