package DP.Transformations;

import DP.Database.DatabaseMetadata;

public interface ITransformation {
    Query transformQuery(final DatabaseMetadata metadata, Query query);
    boolean shouldTransform(Query query);
}
