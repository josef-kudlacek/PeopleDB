package eu.kudljo.peopledb.repository;

import eu.kudljo.peopledb.exception.UnableToSaveException;
import eu.kudljo.peopledb.model.Person;

import java.math.BigDecimal;
import java.sql.*;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.Arrays;
import java.util.Optional;

import static java.util.stream.Collectors.joining;

public class PeopleRepository {
    public static final String SAVE_PERSON_SQL = "INSERT INTO PEOPLE (FIRST_NAME, LAST_NAME, DOB) VALUES (?, ?, ?)";
    private static final String FIND_PERSON_BY_ID_SQL = "SELECT ID, FIRST_NAME, LAST_NAME, DOB, SALARY FROM PEOPLE WHERE ID = ?";
    private static final String COUNT_PEOPLE_SQL = "SELECT COUNT(*) AS COUNT FROM PEOPLE";
    private static final String DELETE_PERSON_BY_ID_SQL = "DELETE FROM PEOPLE WHERE ID = ?";
    private final Connection connection;
    private final String DELETE_PEOPLE_BY_ID_SQL = "DELETE FROM PEOPLE WHERE ID IN (:ids)";

    public PeopleRepository(Connection connection) {
        this.connection = connection;
    }

    public Person save(Person person) throws UnableToSaveException {
        try {
            PreparedStatement preparedStatement = connection.prepareStatement(SAVE_PERSON_SQL, PreparedStatement.RETURN_GENERATED_KEYS);
            preparedStatement.setString(1, person.getFirstName());
            preparedStatement.setString(2, person.getLastName());
            preparedStatement.setTimestamp(3, convertDobToTimestamp(person.getDob())
            );
            int recordsAffected = preparedStatement.executeUpdate();
            ResultSet resultSet = preparedStatement.getGeneratedKeys();
            while (resultSet.next()) {
                long id = resultSet.getLong(1);
                person.setId(id);
                System.out.println(person);
            }
            System.out.printf("Records affected: %d%n", recordsAffected);
        } catch (SQLException e) {
            e.printStackTrace();
            throw new UnableToSaveException("Tried to save person: " + person);
        }
        return person;
    }

    public Optional<Person> findById(Long id) {
        Person person = null;

        try {
            PreparedStatement preparedStatement = connection.prepareStatement(FIND_PERSON_BY_ID_SQL);
            preparedStatement.setLong(1, id);
            ResultSet resultSet = preparedStatement.executeQuery();
            while (resultSet.next()) {
                person = extractPersonFromResultSet(resultSet);
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }

        return Optional.ofNullable(person);
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

    private static Person extractPersonFromResultSet(ResultSet resultSet) throws SQLException {
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
