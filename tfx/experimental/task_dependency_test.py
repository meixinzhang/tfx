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
"""Tests for tfx.experimental.task_dependency."""

# TODO(b/149535307): Remove __future__ imports
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import tensorflow as tf
from tfx.components.base import base_component
from tfx.components.base import base_executor
from tfx.components.base import executor_spec
from tfx.experimental.task_dependency import add_task_dependency
from tfx.orchestration import pipeline
from tfx.types import Artifact
from tfx.types import Channel
from tfx.types import ComponentSpec
from tfx.types.component_spec import ChannelParameter


class _ArtifactTypeA(Artifact):
  TYPE_NAME = 'ArtifactTypeA'


class _ArtifactTypeB(Artifact):
  TYPE_NAME = 'ArtifactTypeB'


class _ArtifactTypeC(Artifact):
  TYPE_NAME = 'ArtifactTypeC'


class _FakeComponentSpecA(ComponentSpec):
  PARAMETERS = {}
  INPUTS = {}
  OUTPUTS = {'output': ChannelParameter(type=_ArtifactTypeA)}


class _FakeComponentSpecB(ComponentSpec):
  PARAMETERS = {}
  INPUTS = {'a': ChannelParameter(type=_ArtifactTypeA)}
  OUTPUTS = {'output': ChannelParameter(type=_ArtifactTypeB)}


class _FakeComponentSpecC(ComponentSpec):
  PARAMETERS = {}
  INPUTS = {'a': ChannelParameter(type=_ArtifactTypeA)}
  OUTPUTS = {'output': ChannelParameter(type=_ArtifactTypeC)}


class _FakeComponent(base_component.BaseComponent):

  SPEC_CLASS = ComponentSpec
  EXECUTOR_SPEC = executor_spec.ExecutorClassSpec(base_executor.BaseExecutor)

  def __init__(self, spec: ComponentSpec):
    instance_name = spec.__class__.__name__.replace('_FakeComponentSpec',
                                                    '').lower()
    super(_FakeComponent, self).__init__(spec=spec, instance_name=instance_name)


class TaskDependencyTest(tf.test.TestCase):

  def testRun(self):
    component_a = _FakeComponent(
        _FakeComponentSpecA(output=Channel(type=_ArtifactTypeA)))
    component_b = _FakeComponent(
        _FakeComponentSpecB(
            a=component_a.outputs['output'],
            output=Channel(type=_ArtifactTypeB)))
    component_c = _FakeComponent(
        _FakeComponentSpecC(
            a=component_a.outputs['output'],
            output=Channel(type=_ArtifactTypeC)))

    add_task_dependency(component_c, [component_b])

    test_pipeline = pipeline.Pipeline(
        pipeline_name='pipeline',
        pipeline_root='root',
        components=[component_c, component_b, component_a])

    self.assertEqual({component_c}, component_b.downstream_nodes)
    self.assertEqual({component_a, component_b}, component_c.upstream_nodes)
    # Should be topologically sorted
    self.assertEqual([component_a, component_b, component_c],
                     test_pipeline.components)


if __name__ == '__main__':
  tf.test.main()
