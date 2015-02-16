angular.module( 'turbine.pipeline', [
  'ui.router',
  'nvd3ChartDirectives'
])
.config(function config( $stateProvider ) {
  $stateProvider
    .state('pipeline', {
      abstract: true,
      url: '/pipeline',
      views: {
        "main": {
          templateUrl: 'pipeline/pipeline.tpl.html'
        }
      },
      data:{ pageTitle: 'Pipelines' }
    })
    .state('pipeline.list', {
      url: '',
      controller: 'PipelineListCtrl',
      templateUrl: 'pipeline/pipeline.list.tpl.html'
    })
    .state('pipeline.detail', {
      url: '/{pipelineId}',
      controller: 'PipelineDetailCtrl',
      templateUrl: 'pipeline/pipeline.detail.tpl.html'
    })
    .state('pipeline.new', {
      url: "/new",
      controller: 'PipelineCreationCtrl',
      templateUrl: 'pipeline/pipeline.new.tpl.html'
    })
    ;
})
.controller( 'PipelineListCtrl', [ "$scope", "Pipelines", "Statistics", function( $scope, Pipelines, Statistics ) {
  $scope.pipelines = Pipelines.query(function(values) {
    angular.forEach(values, function(pipeline) {
      var processedStatistics = [];
      angular.forEach(pipeline.statistic.statistics, function(statisticElement) {
        processedStatistics.push([ statisticElement.date, statisticElement.intake ]);
      });
      pipeline.statistic.values = [{
        "key": "Datapoints",
        "values": processedStatistics
      }];
      console.log(pipeline);
    });
  });
}])
.controller( 'PipelineDetailCtrl', [ "$scope", '$state', "Pipelines", "Statistics", function( $scope, $state, Pipelines, Statistics ) {
  $scope.pipeline = Pipelines.get({pipelineId:$state.params.pipelineId}, function(pipeline){
    var processedStatistics = [];
    angular.forEach(pipeline.statistic.statistics, function(statisticElement) {
      processedStatistics.push([ statisticElement.date, statisticElement.intake ]);
    });
    pipeline.statistic.values = [{
      "key": "Datapoints",
      "values": processedStatistics
    }];
  });

  
}])
.controller( 'PipelineCreationCtrl', [ "$scope", "Pipelines", function( $scope,Pipelines ) {
  $scope.createPipeline = function() {
    var pipeline = {
      name: $scope.name,
      description: $scope.description,
      active: $scope.active
    };
    console.log("Creating pipeline",pipeline);
    Pipelines.save(pipeline);
  };

}]);
