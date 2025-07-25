/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.searchrelevance.stats.common;

import org.opensearch.Version;

/**
 * Interface for objects that hold stat name, path, and type information.
 * The stat name is used as the unique identifier for the stat. It can be used as a request parameter for user filtering.
 */
public interface StatName {
    /**
     * Gets the name of the stat. These must be unique to support user request stat filtering.
     * @return the name of the stat
     */
    String getNameString();

    /**
     * Gets the path of the stat in dot notation.
     * The path must be unique and avoid collisions with other stat names.
     * @return the path of the stat
     */
    String getFullPath();

    /**
     * The type of the stat
     * @return the stat type
     */
    StatType getStatType();

    /**
     * The release version the stat that it was added in
     * @return the version the stat was added
     */
    Version version();
}
