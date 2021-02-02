package DP.Transformations;

import DP.Database.DatabaseMetadata;

public interface ITransformation {

    public Query transformQuery(final DatabaseMetadata metadata, Query query);
}
