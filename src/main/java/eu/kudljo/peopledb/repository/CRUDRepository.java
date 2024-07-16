package eu.kudljo.peopledb.repository;

import eu.kudljo.peopledb.model.Entity;

import java.sql.*;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;

import static java.util.stream.Collectors.joining;

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

    public List<T> findAll() {
        List<T> entities = new ArrayList<>();

        try {
            PreparedStatement preparedStatement = connection.prepareStatement(getFindAllSql());
            ResultSet resultSet = preparedStatement.executeQuery();
            while (resultSet.next()) {
                entities.add(extractEntityFromResultSet(resultSet));
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }

        return entities;
    }

    public long count() {
        long count = 0;
        try {
            PreparedStatement preparedStatement = connection.prepareStatement(getCountSql());
            ResultSet resultSet = preparedStatement.executeQuery();
            if (resultSet.next()) {
                count = resultSet.getLong("COUNT");
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }

        return count;
    }

    public void delete(T entity) {
        try {
            PreparedStatement preparedStatement = connection.prepareStatement(getDeleteSql());
            preparedStatement.setLong(1, entity.getId());
            int affectedRecordCount = preparedStatement.executeUpdate();
            System.out.println(affectedRecordCount);
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    public void delete(T... entities) {
        try {
            Statement statement = connection.createStatement();
            String ids = Arrays.stream(entities)
                    .map(T::getId)
                    .map(String::valueOf)
                    .collect(joining(", "));
            int affectedRecordCount = statement.executeUpdate(getDeleteInSql().replace(":ids", ids));
            System.out.println(affectedRecordCount);
        } catch (SQLException e) {
            e.printStackTrace();
        }

    }

    public void update(T entity) {
        try {
            PreparedStatement preparedStatement = connection.prepareStatement(GetUpdateSql());
            mapForUpdate(entity, preparedStatement);
            preparedStatement.executeUpdate();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    protected abstract String GetUpdateSql();

    /**
     *
     * @return Should return a SQL String like:
     * "DELETE FROM PEOPLE WHERE ID IN (:ids)"
     * Be sure to include the '(:ids)' named parameter & call it 'ids'
     */
    protected abstract String getDeleteInSql();

    protected abstract String getDeleteSql();

    protected abstract String getCountSql();

    protected abstract String getFindAllSql();

    abstract void mapForSave(T entity, PreparedStatement preparedStatement) throws SQLException;

    abstract void mapForUpdate(T entity, PreparedStatement preparedStatement) throws SQLException;

    abstract String getSaveSql();

    /**
     *
     * @return Returns String that represents the SQL needed to retrieve one entity.
     * The SQL must contain one SQL parameter, i.e. "?", that will bind to the entity's ID.
     */
    protected abstract String getFindByIdSql();

    abstract T extractEntityFromResultSet(ResultSet resultSet) throws SQLException;
}
