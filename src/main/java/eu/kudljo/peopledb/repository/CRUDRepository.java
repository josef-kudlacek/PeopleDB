package eu.kudljo.peopledb.repository;

import eu.kudljo.peopledb.model.Entity;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Optional;

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

    public Optional<T> findById(Long id) {
        T entity = null;

        try {
            PreparedStatement preparedStatement = connection.prepareStatement(getFindByIdSql());
            preparedStatement.setLong(1, id);
            ResultSet resultSet = preparedStatement.executeQuery();
            while (resultSet.next()) {
                entity = extractEntityFromResultSet(resultSet);
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }

        return Optional.ofNullable(entity);
    }

    abstract void mapForSave(T entity, PreparedStatement preparedStatement) throws SQLException;

    abstract String getSaveSql();

    /**
     *
     * @return Returns String that represents the SQL needed to retrieve one entity.
     * The SQL must contain one SQL parameter, i.e. "?", that will bind to the entity's ID.
     */
    protected abstract String getFindByIdSql();

    abstract T extractEntityFromResultSet(ResultSet resultSet) throws SQLException;
}
