package eu.kudljo.peopledb.repository;

import eu.kudljo.peopledb.model.Person;

import java.math.BigDecimal;
import java.sql.*;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.Arrays;

import static java.util.stream.Collectors.joining;

public class PeopleRepository extends CRUDRepository<Person> {
    public static final String SAVE_PERSON_SQL = "INSERT INTO PEOPLE (FIRST_NAME, LAST_NAME, DOB) VALUES (?, ?, ?)";
    private static final String FIND_PERSON_BY_ID_SQL = "SELECT ID, FIRST_NAME, LAST_NAME, DOB, SALARY FROM PEOPLE WHERE ID = ?";
    private static final String COUNT_PEOPLE_SQL = "SELECT COUNT(*) AS COUNT FROM PEOPLE";
    private static final String DELETE_PERSON_BY_ID_SQL = "DELETE FROM PEOPLE WHERE ID = ?";
    private final String DELETE_PEOPLE_BY_ID_SQL = "DELETE FROM PEOPLE WHERE ID IN (:ids)";

    public PeopleRepository(Connection connection) {
        super(connection);
    }

    @Override
    String getSaveSql() {
        return SAVE_PERSON_SQL;
    }

    @Override
    void mapForSave(Person entity, PreparedStatement preparedStatement) throws SQLException {
        preparedStatement.setString(1, entity.getFirstName());
        preparedStatement.setString(2, entity.getLastName());
        preparedStatement.setTimestamp(3, convertDobToTimestamp(entity.getDob()));
    }

    @Override
    protected String getFindByIdSql() {
        return FIND_PERSON_BY_ID_SQL;
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

    public long count() {
        long count = 0;
        try {
            PreparedStatement preparedStatement = connection.prepareStatement(COUNT_PEOPLE_SQL);
            ResultSet resultSet = preparedStatement.executeQuery();
            if (resultSet.next()) {
                count = resultSet.getLong("COUNT");
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }

        return count;
    }

    public void delete(Person person) {
        try {
            PreparedStatement preparedStatement = connection.prepareStatement(DELETE_PERSON_BY_ID_SQL);
            preparedStatement.setLong(1, person.getId());
            int affectedRecordCount = preparedStatement.executeUpdate();
            System.out.println(affectedRecordCount);
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    public void delete(Person... people) {
        try {
            Statement statement = connection.createStatement();
            String ids = Arrays.stream(people)
                    .map(Person::getId)
                    .map(String::valueOf)
                    .collect(joining(", "));
            int affectedRecordCount = statement.executeUpdate(DELETE_PEOPLE_BY_ID_SQL.replace(":ids", ids));
            System.out.println(affectedRecordCount);
        } catch (SQLException e) {
            e.printStackTrace();
        }

    }

    public void update(Person person) {
        try {
            PreparedStatement preparedStatement = connection.prepareStatement(
                    "UPDATE PEOPLE SET FIRST_NAME = ?, LAST_NAME = ?, DOB = ?, SALARY = ? WHERE ID = ?");
            preparedStatement.setString(1, person.getFirstName());
            preparedStatement.setString(2, person.getLastName());
            preparedStatement.setTimestamp(3, convertDobToTimestamp(person.getDob()));
            preparedStatement.setBigDecimal(4, person.getSalary());
            preparedStatement.setLong(5, person.getId());
            preparedStatement.executeUpdate();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    private static Timestamp convertDobToTimestamp(ZonedDateTime dob) {
        return Timestamp.valueOf(dob.withZoneSameInstant(ZoneId.of("+0")).toLocalDateTime());
    }
}
