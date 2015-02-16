angular.module( 'turbine', [
  'ngResource',
  'templates-app',
  'templates-common',
  'turbine.home',
  'turbine.pipeline',
  'turbine.login',
  'turbine.account',
  'ui.router',
  'nvd3ChartDirectives'
])
.config( function myAppConfig ( $stateProvider, $urlRouterProvider ) {
  $urlRouterProvider.otherwise( '/home' );
})
.run( function run () {
})
.controller( 'AppCtrl', function AppCtrl ( $scope, $location ) {
  $scope.$on('$stateChangeSuccess', function(event, toState, toParams, fromState, fromParams){
    if ( angular.isDefined( toState.data.pageTitle ) ) {
      $scope.pageTitle = toState.data.pageTitle + ' | turbine.io' ;
    }
  });
  $scope.colorFunction = function() {
    return function(d, i) {
        return '#12b0c5';
    };
  };
})
.factory('Pipelines', ['$resource',function($resource) {
  return $resource('/api/v1/pipelines/:pipelineId', {pipelineId:'@id'}, {
  });
}])
.factory('Statistics', ['$resource',function($resource) {
  return $resource('/api/v1/pipelines/:pipelineId/statistics', {pipelineId:'@id'}, {
  });
}])
;
