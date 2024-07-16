package eu.kudljo.peopledb.repository;

import eu.kudljo.peopledb.model.Entity;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

abstract class CRUDRepository<T extends Entity> {
    protected Connection connection;

    public CRUDRepository(Connection connection) {
        this.connection = connection;
    }

    public T save(T entity) {
        try {
            PreparedStatement preparedStatement = connection.prepareStatement(getSaveSql(), PreparedStatement.RETURN_GENERATED_KEYS);
            mapForSave(entity, preparedStatement);
            int recordsAffected = preparedStatement.executeUpdate();
            ResultSet resultSet = preparedStatement.getGeneratedKeys();
            while (resultSet.next()) {
                long id = resultSet.getLong(1);
                entity.setId(id);
                System.out.println(entity);
            }
            System.out.printf("Records affected: %d%n", recordsAffected);
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return entity;
    }

    abstract void mapForSave(T entity, PreparedStatement preparedStatement) throws SQLException;

    abstract String getSaveSql();
}
