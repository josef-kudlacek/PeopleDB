package eu.kudljo.peopledb.repository;

import eu.kudljo.peopledb.annotation.SQL;
import eu.kudljo.peopledb.model.Address;
import eu.kudljo.peopledb.model.CrudOperation;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

public class AddressRepository extends CRUDRepository<Address> {

    public AddressRepository(Connection connection) {
        super(connection);
    }

    @Override
    @SQL(operationType = CrudOperation.SAVE, value = """
            INSERT INTO ADDRESSES (STREET_ADDRESS, ADDRESS2, CITY, STATE, POSTCODE, COUNTY, REGION, COUNTRY)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """)
    void mapForSave(Address entity, PreparedStatement preparedStatement) throws SQLException {
        preparedStatement.setString(1, entity.streetAddress());
        preparedStatement.setString(2, entity.address2());
        preparedStatement.setString(3, entity.city());
        preparedStatement.setString(4, entity.state());
        preparedStatement.setString(5, entity.postcode());
        preparedStatement.setString(6, entity.county());
        preparedStatement.setString(7, entity.region().toString());
        preparedStatement.setString(8, entity.country());
    }

    @Override
    void mapForUpdate(Address entity, PreparedStatement preparedStatement) throws SQLException {

    }

    @Override
    Address extractEntityFromResultSet(ResultSet resultSet) throws SQLException {
        return null;
    }
}
