# Copyright 2019 Google LLC. All Rights Reserved.
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
"""Integration tests for Kubeflow-based orchestrator and GCP backend."""

from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import logging
import os
import sys

import tensorflow as tf

from tfx.components.evaluator.component import Evaluator
from tfx.components.example_gen.csv_example_gen.component import CsvExampleGen
from tfx.components.model_validator.component import ModelValidator
from tfx.components.statistics_gen.component import StatisticsGen
from tfx.components.transform.component import Transform
from tfx.orchestration.kubeflow import test_utils
from tfx.proto import evaluator_pb2
from tfx.types import channel_utils
from tfx.types import standard_artifacts
from tfx.utils import dsl_utils


class KubeflowGCPIntegrationTest(test_utils.BaseKubeflowTest):

  def setUp(self):
    super(KubeflowGCPIntegrationTest, self).setUp()

    # Channel of raw Example artifacts for testing.
    raw_train_examples = standard_artifacts.Examples(split='train')
    raw_train_examples.uri = os.path.join(
        self._intermediate_data_root,
        'csv_example_gen/examples/test-pipeline/train/')
    raw_eval_examples = standard_artifacts.Examples(split='eval')
    raw_eval_examples.uri = os.path.join(
        self._intermediate_data_root,
        'csv_example_gen/examples/test-pipeline/eval/')
    self._test_raw_examples = channel_utils.as_channel(
        [raw_train_examples, raw_eval_examples])

    # Channel of transformed Example artifacts for testing.
    transformed_train_examples = standard_artifacts.Examples(split='train')
    transformed_train_examples.uri = os.path.join(
        self._intermediate_data_root,
        'transform/transformed_examples/test-pipeline/train/')
    transformed_eval_examples = standard_artifacts.Examples(split='eval')
    transformed_eval_examples.uri = os.path.join(
        self._intermediate_data_root,
        'transform/transformed_examples/test-pipeline/eval/')
    self._test_transformed_examples = channel_utils.as_channel(
        [transformed_eval_examples, transformed_eval_examples])

    # Channel of Schema artifact for testing.
    schema = standard_artifacts.Schema()
    schema.uri = os.path.join(self._intermediate_data_root,
                              'schema_gen/output/test-pipeline/')
    self._test_schema = channel_utils.as_channel([schema])

    # Channel of TransformGraph artifact for testing.
    transform_graph = standard_artifacts.TransformGraph()
    transform_graph.uri = os.path.join(
        self._intermediate_data_root,
        'transform/test-pipeline/transform_output/')
    self._test_transform_graph = channel_utils.as_channel([transform_graph])

    # Channel of Model artifact for testing.
    model = standard_artifacts.Model()
    model.uri = os.path.join(self._intermediate_data_root,
                             'trainer/output/test-pipeline/')
    self._test_model = channel_utils.as_channel([model])

    # Channel of ModelBlessing artifact for testing.
    model_blessing = standard_artifacts.ModelBlessing()
    model_blessing.uri = os.path.join(
        self._intermediate_data_root, 'model_validator/blessing/test-pipeline/')
    self._test_model_blessing = channel_utils.as_channel([model_blessing])

  def testCsvExampleGenOnDataflowRunner(self):
    """Test for CsvExampleGen on DataflowRunner invocation."""
    pipeline_name = 'kubeflow-csv-example-gen-dataflow-test-{}'.format(
        self._random_id())
    pipeline = self._create_dataflow_pipeline(pipeline_name, [
        CsvExampleGen(input_base=dsl_utils.csv_input(self._data_root)),
    ])
    self._compile_and_run_pipeline(pipeline)

  def testStatisticsGenOnDataflowRunner(self):
    """Test for StatisticsGen on DataflowRunner invocation."""
    pipeline_name = 'kubeflow-statistics-gen-dataflow-test-{}'.format(
        self._random_id())
    pipeline = self._create_dataflow_pipeline(pipeline_name, [
        StatisticsGen(input_data=self._test_raw_examples),
    ])
    self._compile_and_run_pipeline(pipeline)

  def testTransformOnDataflowRunner(self):
    """Test for Transform on DataflowRunner invocation."""
    pipeline_name = 'kubeflow-transform-dataflow-test-{}'.format(
        self._random_id())
    pipeline = self._create_dataflow_pipeline(pipeline_name, [
        Transform(
            input_data=self._test_raw_examples,
            schema=self._test_schema,
            module_file=self._taxi_module_file),
    ])
    self._compile_and_run_pipeline(pipeline)

  def testEvaluatorOnDataflowRunner(self):
    """Test for Evaluator on DataflowRunner invocation."""
    pipeline_name = 'kubeflow-evaluator-dataflow-test-{}'.format(
        self._random_id())
    pipeline = self._create_dataflow_pipeline(pipeline_name, [
        Evaluator(
            examples=self._test_raw_examples,
            model_exports=self._test_model,
            feature_slicing_spec=evaluator_pb2.FeatureSlicingSpec(specs=[
                evaluator_pb2.SingleSlicingSpec(
                    column_for_slicing=['trip_start_hour'])
            ])),
    ])
    self._compile_and_run_pipeline(pipeline)

  def testModelValidatorOnDataflowRunner(self):
    """Test for ModelValidatorEvaluator on DataflowRunner invocation."""
    pipeline_name = 'kubeflow-model-validator-dataflow-test-{}'.format(
        self._random_id())
    pipeline = self._create_dataflow_pipeline(pipeline_name, [
        ModelValidator(
            examples=self._test_raw_examples, model=self._test_model),
    ])
    self._compile_and_run_pipeline(pipeline)

  # TODO(muchida): Add test cases for AI Platform Trainer and Pusher.


if __name__ == '__main__':
  logging.basicConfig(stream=sys.stdout, level=logging.INFO)
  tf.test.main()