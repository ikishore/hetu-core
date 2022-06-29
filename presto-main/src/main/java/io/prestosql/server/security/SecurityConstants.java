/*
 * Copyright (C) 2018-2020. Huawei Technologies Co., Ltd. All rights reserved.
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.prestosql.server.security;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class SecurityConstants
{
    /**
     * Http disable methods list
     */
    public static final List<String> HTTP_DISABLE_METHOD_LIST = Collections.unmodifiableList(new ArrayList<String>()
    {
        {
            this.add("OPTIONS");
        }
    });

    private SecurityConstants()
    {
    }
}
