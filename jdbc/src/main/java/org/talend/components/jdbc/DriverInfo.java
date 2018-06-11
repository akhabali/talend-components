// ============================================================================
//
// Copyright (C) 2006-2017 Talend Inc. - www.talend.com
//
// This source code is available under agreement available at
// %InstallDIR%\features\org.talend.rcp.branding.%PRODUCTNAME%\%PRODUCTNAME%license.txt
//
// You should have received a copy of the agreement
// along with this program; if not, write to Talend SA
// 9 rue Pages 92150 Suresnes, France
//
// ============================================================================
package org.talend.components.jdbc;

import java.util.ArrayList;
import java.util.List;

import javax.json.bind.annotation.JsonbProperty;

import lombok.Data;

/**
 * the bean class for the database information
 */
@Data
public class DriverInfo {

    private String id;

    @JsonbProperty("class")
    private String clazz;

    private String url;

    private List<Path> paths = new ArrayList<>();

    @Data
    public static class Path {

        private String path;
    }

}
