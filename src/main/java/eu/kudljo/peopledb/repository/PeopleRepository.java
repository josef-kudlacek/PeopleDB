package eu.kudljo.peopledb.repository;

import eu.kudljo.peopledb.annotation.SQL;
import eu.kudljo.peopledb.model.Address;
import eu.kudljo.peopledb.model.CrudOperation;
import eu.kudljo.peopledb.model.Person;
import eu.kudljo.peopledb.model.Region;

import java.math.BigDecimal;
import java.sql.*;
import java.time.ZoneId;
import java.time.ZonedDateTime;

public class PeopleRepository extends CRUDRepository<Person> {
    private AddressRepository addressRepository = null;
    public static final String SAVE_PERSON_SQL = """
            INSERT INTO PEOPLE
            (FIRST_NAME, LAST_NAME, DOB, SALARY, EMAIL, HOME_ADDRESS)
            VALUES (?, ?, ?, ?, ?, ?)
            """;
    private static final String FIND_PERSON_BY_ID_SQL = """
            SELECT
            P.ID, P.FIRST_NAME, P.LAST_NAME, P.DOB, P.SALARY, P.HOME_ADDRESS,
            A.ID AS A_ID, A.STREET_ADDRESS, A.ADDRESS2, A.CITY, A.STATE, A.POSTCODE, A.COUNTY, A.REGION, A.COUNTRY
            FROM PEOPLE AS P
            LEFT OUTER JOIN ADDRESSES AS A ON P.HOME_ADDRESS = A.ID
            WHERE P.ID = ?
            """;
    private static final String FIND_ALL_SQL = "SELECT ID, FIRST_NAME, LAST_NAME, DOB, SALARY, HOME_ADDRESS FROM PEOPLE";
    private static final String COUNT_PEOPLE_SQL = "SELECT COUNT(*) AS COUNT FROM PEOPLE";
    private static final String DELETE_PERSON_BY_ID_SQL = "DELETE FROM PEOPLE WHERE ID = ?";
    private static final String DELETE_PEOPLE_BY_IDS_SQL = "DELETE FROM PEOPLE WHERE ID IN (:ids)";
    private static final String UPDATE_PERSON_BY_ID_SQL = "UPDATE PEOPLE SET FIRST_NAME = ?, LAST_NAME = ?, DOB = ?, SALARY = ? WHERE ID = ?";

    public PeopleRepository(Connection connection) {
        super(connection);
        addressRepository = new AddressRepository(connection);
    }

    @Override
    @SQL(value = SAVE_PERSON_SQL, operationType = CrudOperation.SAVE)
    void mapForSave(Person person, PreparedStatement preparedStatement) throws SQLException {
        Address savedAddress = null;
        preparedStatement.setString(1, person.getFirstName());
        preparedStatement.setString(2, person.getLastName());
        preparedStatement.setTimestamp(3, convertDobToTimestamp(person.getDob()));
        preparedStatement.setBigDecimal(4, person.getSalary());
        preparedStatement.setString(5, person.getEmail());
        if (person.getHomeAddress().isPresent()) {
            savedAddress = addressRepository.save(person.getHomeAddress().get());
            preparedStatement.setLong(6, savedAddress.id());
        } else {
            preparedStatement.setObject(6, null);
        }
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
        Person person = new Person(personId, firstName, lastName, dob, salary);
        Address address = extractAddress(resultSet);
        person.setHomeAddress(address);
        return person;
    }

    private Address extractAddress(ResultSet resultSet) throws SQLException {
        if (resultSet.getObject("A_ID") == null) {
            return null;
        }
        long addressId = resultSet.getLong("A_ID");
        String streetAddress = resultSet.getString("STREET_ADDRESS");
        String address2 = resultSet.getString("ADDRESS2");
        String city = resultSet.getString("CITY");
        String state = resultSet.getString("STATE");
        String postcode = resultSet.getString("POSTCODE");
        String county = resultSet.getString("COUNTY");
        Region region = Region.valueOf(resultSet.getString("REGION").toUpperCase());
        String country = resultSet.getString("COUNTRY");
        return new Address(addressId, streetAddress, address2, city, state, postcode, country, county, region);
    }

    private static Timestamp convertDobToTimestamp(ZonedDateTime dob) {
        return Timestamp.valueOf(dob.withZoneSameInstant(ZoneId.of("+0")).toLocalDateTime());
    }
}
