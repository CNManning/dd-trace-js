'use strict'

const agent = require('../../dd-trace/test/plugins/agent')
const plugin = require('../src')

wrapIt()

describe('Plugin', () => {
  let couchbase
  let N1qlQuery
  let ViewQuery
  let SearchQuery
  let CbasQuery
  let cluster
  let bucket
  let bucketManager
  let platform
  let tracer

  describe('couchbase', () => {
    withVersions(plugin, 'couchbase', version => {
      beforeEach(() => {
        platform = require('../../dd-trace/src/platform')
        tracer = require('../../dd-trace')
      })

      describe('without configuration', () => {
        beforeEach(() => {
          return agent.load(plugin, 'couchbase').then(() => {
            couchbase = require(`../../../versions/couchbase@${version}`).get()
            N1qlQuery = couchbase.N1qlQuery
            ViewQuery = couchbase.ViewQuery
            SearchQuery = couchbase.SearchQuery
            CbasQuery = couchbase.CbasQuery
          })
        })

        beforeEach(done => {
          cluster = new couchbase.Cluster('couchbase://localhost/')
          cluster.authenticate('Administrator', 'password')
          bucket = cluster.openBucket('datadog-test', (err) => {
            if (err) done(err)

            bucketManager = bucket.manager()
            const map = `function (doc, meta) {
                          if (doc.type && doc.type == 'landmark') {
                            emit(doc.name, null)
                          }
                        }`
            const ddocdata = { views: { by_name: { map } } }

            bucketManager.upsertDesignDocument('datadoc', ddocdata, (err) => {
              done(err)
            })
          })
        })

        afterEach(() => {
          bucket.disconnect()
        })

        after(() => {
          return agent.close()
        })

        describe('queries on cluster', () => {
          it('should handle N1ql queries', done => {
            const query = `UPSERT INTO \`datadog-test\` ( KEY, VALUE )
                            VALUES (
                              "landmark_1",
                              {
                                "id": "1",
                                "type": "landmark",
                                "name": "La Tour Eiffel",
                                "location": "France"
                              }
                            )`

            const n1qlQuery = N1qlQuery.fromString(query)

            agent
              .use(traces => {
                const span = traces[0][0]
                expect(span).to.have.property('name', 'couchbase.call')
                expect(span).to.have.property('service', 'test-couchbase')
                expect(span).to.have.property('resource', query)
                expect(span).to.have.property('type', 'sql')
                expect(span.meta).to.have.property('span.kind', 'client')
                expect(span.meta).to.have.property('bucket', 'datadog-test')
                expect(span.meta).to.have.property('query.type', 'n1ql')
              })
              .then(done)
              .catch(done)

            cluster.query(n1qlQuery, (err, rows, meta) => {
              if (err) done(err)
            })
          })

          it('should handle Search queries', done => {
            const index = 'test'
            const searchQuery = SearchQuery.new(index, SearchQuery.queryString('eiffel'))

            agent
              .use(traces => {
                const span = traces[0][0]
                expect(span).to.have.property('name', 'couchbase.call')
                expect(span).to.have.property('service', 'test-couchbase')
                expect(span).to.have.property('resource', index)
                expect(span).to.have.property('type', 'sql')
                expect(span.meta).to.have.property('span.kind', 'client')
                expect(span.meta).to.have.property('bucket', 'datadog-test')
                expect(span.meta).to.have.property('query.type', 'search')
              })
              .then(done)
              .catch(done)

            cluster.query(searchQuery, (err, rows, meta) => {
              if (err) done(err)
              expect(rows.length).to.not.equal(0)
            })
          })

          it('should handle cbas queries', done => {
            const query = 'SELECT * FROM airlines'
            const cbasQuery = CbasQuery.fromString(query)

            agent
              .use(traces => {
                const span = traces[0][0]
                expect(span).to.have.property('name', 'couchbase.call')
                expect(span).to.have.property('service', 'test-couchbase')
                expect(span).to.have.property('resource', query)
                expect(span).to.have.property('type', 'sql')
                expect(span.meta).to.have.property('span.kind', 'client')
                expect(span.meta).to.have.property('bucket', 'datadog-test')
                expect(span.meta).to.have.property('query.type', 'cbas')
              })
              .then(done)
              .catch(done)

            cluster.query(cbasQuery, (err, rows, meta) => {
              if (err) done(err)
            })
          })
        })

        describe('queries on buckets', () => {
          it('should handle N1ql queries', done => {
            const query = 'SELECT 1+1'
            const n1qlQuery = N1qlQuery.fromString(query)

            agent
              .use(traces => {
                const span = traces[0][0]
                expect(span).to.have.property('name', 'couchbase.call')
                expect(span).to.have.property('service', 'test-couchbase')
                expect(span).to.have.property('resource', query)
                expect(span).to.have.property('type', 'sql')
                expect(span.meta).to.have.property('span.kind', 'client')
                expect(span.meta).to.have.property('bucket', 'datadog-test')
                expect(span.meta).to.have.property('query.type', 'n1ql')
              })
              .then(done)
              .catch(done)

            cluster.query(n1qlQuery, (err, rows, meta) => {
              if (err) done(err)
            })
          })

          it('should handle View queries ', done => {
            const viewQuery = ViewQuery.from('datadoc', 'by_name')

            agent
              .use(traces => {
                const span = traces[0][0]
                expect(span).to.have.property('name', 'couchbase.call')
                expect(span).to.have.property('service', 'test-couchbase')
                expect(span).to.have.property('resource', viewQuery.name)
                expect(span).to.have.property('type', 'sql')
                expect(span.meta).to.have.property('span.kind', 'client')
                expect(span.meta).to.have.property('bucket', 'datadog-test')
                expect(span.meta).to.have.property('ddoc', viewQuery.ddoc)
                expect(span.meta).to.have.property('query.type', 'view')
              })
              .then(done)
              .catch(done)

            bucket.query(viewQuery, (err, rows, meta) => {
              if (err) done(err)
              expect(rows.length).to.not.equal(0)
            })
          })
        })
      })
    })
  })
})
