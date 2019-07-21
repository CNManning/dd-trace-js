'use strict'

const Tags = require('../../../ext/tags')
const Kinds = require('../../../ext/kinds')
const analyticsSampler = require('../../dd-trace/src/analytics_sampler')

function startQuerySpan (queryType, resource, tracer, config) {
  const childOf = tracer.scope().active()
  const span = tracer.startSpan(`couchbase.call`, {
    childOf,
    tags: {
      'db.type': 'couchbase',
      'span.type': 'sql',
      'component': 'couchbase',
      'service.name': config.service || `${tracer._service}-couchbase`,
      'resource.name': resource,
      'query.type': queryType,
      [Tags.SPAN_KIND]: Kinds.CLIENT
    }
  })

  analyticsSampler.sample(span, config.analytics)
  return span
}

function onRequestFinish (emitter, span) {
  const errorListener = (err) => {
    span.finish()
    span.setTag(Tags.ERROR, err)
  }
  const rowsListener = () => {
    span.finish()
  }

  emitter.once('rows', () => {
    rowsListener()
    emitter.removeListener('error', errorListener)
  })
  emitter.once('error', (err) => {
    errorListener(err)
    emitter.removeListener('rows', rowsListener)
  })
}

function createWrapN1qlQuery (tracer, config) {
  return function wrapQuery (_n1qlReq) {
    return function queryWithTrace (host, q, adhoc, emitter) {
      const scope = tracer.scope()
      const query = q.statement
      const bucket = this.name
      const span = startQuerySpan('n1ql', query, tracer, config)

      span.addTags({
        'cluster.host': host,
        bucket
      })

      onRequestFinish(emitter, span)

      return scope.bind(_n1qlReq, span).apply(this, arguments)
    }
  }
}

function createWrapViewQuery (tracer, config) {
  return function wrapQuery (_viewReq) {
    return function queryWithTrace () {
      const ddoc = arguments[1]
      const viewName = arguments[2]

      const scope = tracer.scope()
      const span = startQuerySpan('view', viewName, tracer, config)

      const bucket = this.name
      span.addTags({
        ddoc,
        bucket
      })

      onRequestFinish(arguments[_viewReq.length - 1], span)

      return scope.bind(_viewReq, span).apply(this, arguments)
    }
  }
}

function createWrapFtsQuery (tracer, config) {
  return function wrapQuery (_ftsReq) {
    return function queryWithTrace (q, emitter) {
      const scope = tracer.scope()
      const index = q.data.indexName
      const span = startQuerySpan('search', index, tracer, config)

      const bucket = this.name
      span.setTag('bucket', bucket)

      onRequestFinish(emitter, span)

      return scope.bind(_ftsReq, span).apply(this, arguments)
    }
  }
}

function createWrapCbasQuery (tracer, config) {
  return function wrapQuery (_cbasReq) {
    return function queryWithTrace (host, q, emitter) {
      const scope = tracer.scope()
      const query = q.statement
      const bucket = this.name
      const span = startQuerySpan('cbas', query, tracer, config)

      span.addTags({
        'cbas.host': host,
        bucket
      })

      onRequestFinish(emitter, span)

      return scope.bind(_cbasReq, span).apply(this, arguments)
    }
  }
}

function createWrapOpenBucket (tracer) {
  return function wrapOpenBucket (openBucket) {
    return function openBucketWithTrace () {
      const bucket = openBucket.apply(this, arguments)
      return tracer.scope().bind(bucket)
    }
  }
}

module.exports = [
  {
    name: 'couchbase',
    versions: ['>=2.2.0'],
    file: 'lib/bucket.js',
    patch (Bucket, tracer, config) {
      this.wrap(Bucket.prototype, '_n1qlReq', createWrapN1qlQuery(tracer, config))
      this.wrap(Bucket.prototype, '_viewReq', createWrapViewQuery(tracer, config))
      this.wrap(Bucket.prototype, '_ftsReq', createWrapFtsQuery(tracer, config))
      this.wrap(Bucket.prototype, '_cbasReq', createWrapCbasQuery(tracer, config))
    },
    unpatch (Bucket) {
      this.unwrap(Bucket.prototype, '_n1qlReq')
      this.unwrap(Bucket.prototype, '_viewReq')
      this.unwrap(Bucket.prototype, '_ftsReq')
      this.unwrap(Bucket.prototype, '_cbasReq')
    }
  },
  {
    name: 'couchbase',
    versions: ['>=2.2.0'],
    file: 'lib/cluster.js',
    patch (Cluster, tracer, config) {
      this.wrap(Cluster.prototype, 'openBucket', createWrapOpenBucket(tracer, config))
    },
    unpatch (Cluster) {
      this.unwrap(Cluster.prototype, 'openBucket')
    }
  }
]
