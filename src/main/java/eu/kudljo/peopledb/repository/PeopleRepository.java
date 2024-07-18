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
import java.util.Optional;

public class PeopleRepository extends CRUDRepository<Person> {
    private AddressRepository addressRepository;
    public static final String SAVE_PERSON_SQL = """
            INSERT INTO PEOPLE
            (FIRST_NAME, LAST_NAME, DOB, SALARY, EMAIL, HOME_ADDRESS, BUSINESS_ADDRESS, SPOUSE, PARENT)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """;
    private static final String FIND_PERSON_BY_ID_SQL = """
            SELECT
            P.ID AS P_ID, P.FIRST_NAME AS P_FIRST_NAME, P.LAST_NAME AS P_LAST_NAME, P.DOB AS P_DOB, P.SALARY AS P_SALARY,
            P.HOME_ADDRESS AS P_HOME_ADDRESS, P.BUSINESS_ADDRESS AS P_BUSINESS_ADDRESS,
            HOME.ID AS HOME_ID, HOME.STREET_ADDRESS AS HOME_STREET_ADDRESS, HOME.ADDRESS2 AS HOME_ADDRESS2, HOME.CITY AS HOME_CITY,
            HOME.STATE AS HOME_STATE, HOME.POSTCODE AS HOME_POSTCODE, HOME.COUNTY AS HOME_COUNTY, HOME.REGION AS HOME_REGION,
            HOME.COUNTRY AS HOME_COUNTRY,
            BIZ.ID AS BIZ_ID, BIZ.STREET_ADDRESS AS BIZ_STREET_ADDRESS, BIZ.ADDRESS2 AS BIZ_ADDRESS2, BIZ.CITY AS BIZ_CITY,
            BIZ.STATE AS BIZ_STATE, BIZ.POSTCODE AS BIZ_POSTCODE, BIZ.COUNTY AS BIZ_COUNTY, BIZ.REGION AS BIZ_REGION,
            BIZ.COUNTRY AS BIZ_COUNTRY,
            SPOUSE.ID AS S_ID, SPOUSE.FIRST_NAME AS S_FIRST_NAME, SPOUSE.LAST_NAME AS S_LAST_NAME, SPOUSE.DOB AS S_DOB, SPOUSE.SALARY AS S_SALARY,
            SPOUSE.HOME_ADDRESS AS S_HOME_ADDRESS, SPOUSE.BUSINESS_ADDRESS AS S_BUSINESS_ADDRESS
            FROM PEOPLE AS P
            LEFT OUTER JOIN ADDRESSES AS HOME ON P.HOME_ADDRESS = HOME.ID
            LEFT OUTER JOIN ADDRESSES AS BIZ ON P.BUSINESS_ADDRESS = BIZ.ID
            LEFT OUTER JOIN PEOPLE AS SPOUSE ON P.SPOUSE = SPOUSE.ID
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
        preparedStatement.setString(1, person.getFirstName());
        preparedStatement.setString(2, person.getLastName());
        preparedStatement.setTimestamp(3, convertDobToTimestamp(person.getDob()));
        preparedStatement.setBigDecimal(4, person.getSalary());
        preparedStatement.setString(5, person.getEmail());
        associateAddressWithPerson(preparedStatement, person.getHomeAddress(), 6);
        associateAddressWithPerson(preparedStatement, person.getBusinessAddress(), 7);
        associatePersonWithPerson(preparedStatement, person.getSpouse(), 8);
        associateChildWithPerson(preparedStatement, person.getParent(), 9);
    }

    @Override
    protected void postSave(Person entity, long id) {
        entity.getChildren().stream()
                .forEach(this::save);
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
        Person person = extractPerson(resultSet, "P_");
        Address homeAddress = extractAddress(resultSet, "HOME_");
        person.setHomeAddress(homeAddress);
        Address businessAddress = extractAddress(resultSet, "BIZ_");
        person.setBusinessAddress(businessAddress);
        Person spouse = extractPerson(resultSet, "S_");
        person.setSpouse(spouse);
        return person;
    }

    private Person extractPerson(ResultSet resultSet, String aliasPrefix) throws SQLException {
        Long personId = getValueByAlias(aliasPrefix + "ID", resultSet, Long.class);
        if (personId == null) {
            return null;
        }
        String firstName = getValueByAlias(aliasPrefix + "FIRST_NAME", resultSet, String.class);
        String lastName = getValueByAlias(aliasPrefix + "LAST_NAME", resultSet, String.class);
        ZonedDateTime dob = ZonedDateTime.of(getValueByAlias(aliasPrefix + "DOB", resultSet, Timestamp.class).toLocalDateTime(), ZoneId.of("+0"));
        BigDecimal salary = getValueByAlias(aliasPrefix + "SALARY", resultSet, BigDecimal.class);
        Person person = new Person(personId, firstName, lastName, dob, salary);
        return person;
    }

    private Address extractAddress(ResultSet resultSet, String aliasPrefix) throws SQLException {
        Long addressId = getValueByAlias(aliasPrefix + "ID", resultSet, Long.class);
        if (addressId == null) {
            return null;
        }
        String streetAddress = getValueByAlias(aliasPrefix + "STREET_ADDRESS", resultSet, String.class);
        String address2 = getValueByAlias(aliasPrefix + "ADDRESS2", resultSet, String.class);
        String city = getValueByAlias(aliasPrefix + "CITY", resultSet, String.class);
        String state = getValueByAlias(aliasPrefix + "STATE", resultSet, String.class);
        String postcode = getValueByAlias(aliasPrefix + "POSTCODE", resultSet, String.class);
        String county = getValueByAlias(aliasPrefix + "COUNTY", resultSet, String.class);
        Region region = Region.valueOf(getValueByAlias(aliasPrefix + "REGION", resultSet, String.class).toUpperCase());
        String country = getValueByAlias(aliasPrefix + "COUNTRY", resultSet, String.class);
        return new Address(addressId, streetAddress, address2, city, state, postcode, country, county, region);
    }

    private <T> T getValueByAlias(String alias, ResultSet resultSet, Class<T> clazz) throws SQLException {
        int columnCount = resultSet.getMetaData().getColumnCount();
        for (int columnIndex = 1; columnIndex <= columnCount; columnIndex++) {
            if (alias.equals(resultSet.getMetaData().getColumnLabel(columnIndex))) {
                return (T) resultSet.getObject(columnIndex);
            }
        }

        throw new SQLException(String.format("Column not found for alias: '%s'", alias));
    }

    private static Timestamp convertDobToTimestamp(ZonedDateTime dob) {
        return Timestamp.valueOf(dob.withZoneSameInstant(ZoneId.of("+0")).toLocalDateTime());
    }

    private void associateAddressWithPerson(PreparedStatement preparedStatement, Optional<Address> address, int parameterIndex) throws SQLException {
        Address savedAddress;
        if (address.isPresent()) {
            savedAddress = addressRepository.save(address.get());
            preparedStatement.setLong(parameterIndex, savedAddress.id());
        } else {
            preparedStatement.setObject(parameterIndex, null);
        }
    }

    private void associatePersonWithPerson(PreparedStatement preparedStatement, Optional<Person> person, int parameterIndex) throws SQLException {
        Person savedPerson;
        if (person.isPresent()) {
            savedPerson = this.save(person.get());
            preparedStatement.setLong(parameterIndex, savedPerson.getId());
        } else {
            preparedStatement.setObject(parameterIndex, null);
        }
    }

    private void associateChildWithPerson(PreparedStatement preparedStatement, Optional<Person> person, int parameterIndex) throws SQLException {
        if (person.isPresent()) {
            preparedStatement.setLong(parameterIndex, person.get().getId());
        } else {
            preparedStatement.setObject(parameterIndex, null);
        }
    }
}
