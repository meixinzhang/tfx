# Lint as: python2, python3
# Copyright 2020 Google LLC. All Rights Reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
"""Utility to specify task dependency for TFX components.

This feature is experimental.
The utility is for task dependency: enforcing execution order between components
that do not necessarily share artifacts in a synchronous pipeline.
"""

# TODO(b/149535307): Remove __future__ imports
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

from typing import List
from tfx.components.base import base_node


def add_task_dependency(node: base_node.BaseNode,
                        dependencies: List[base_node.BaseNode]) -> None:
  """Add task dependency for TFX components.

  This experimental function forces execution order between node and
  dependencies.
  Adding a task dependency does not introduce lineage between components.
  Only works in synchronous pipelines.

  Args:
    node: a component that must run after provided dependencies.
    dependencies: a list of components that must run before the node.
  """
  for dep in dependencies:
    node.add_upstream_node(dep)
    dep.add_downstream_node(node)
