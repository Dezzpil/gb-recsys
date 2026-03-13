import { useParams, useSearchParams, Link } from 'react-router-dom'
import { useQuery } from '@tanstack/react-query'
import { getUserInteractions, getUserRecommendations } from '../api/api'
import { Table, Row, Col, Card, Spinner, Alert, Badge } from 'react-bootstrap'
import { FaHistory, FaShoppingBag, FaMagic } from 'react-icons/fa'
import moment from 'moment'

const UserPage = () => {
  const { email } = useParams<{ email: string }>()
  const [searchParams] = useSearchParams()
  const mergeId = searchParams.get('merge_id') ? Number(searchParams.get('merge_id')) : undefined

  const interactionsQuery = useQuery({
    queryKey: ['user-interactions', email, mergeId],
    queryFn: () => getUserInteractions(email!, mergeId)
  })

  const recsQuery = useQuery({
    queryKey: ['user-recommendations', email, mergeId],
    queryFn: () => getUserRecommendations(email!, mergeId)
  })

  const isLoading = interactionsQuery.isLoading || recsQuery.isLoading
  const error = interactionsQuery.error || recsQuery.error

  if (isLoading) return <div className="text-center mt-5"><Spinner animation="border" /></div>
  if (error) return <Alert variant="danger">Ошибка при загрузке данных пользователя</Alert>

  const interactions = interactionsQuery.data
  const recommendations = [...(recsQuery.data?.recommendations || [])].reverse()
  const excludedRecommendations = [...(recsQuery.data?.excluded_recommendations || [])].reverse()

  const renderProductName = (name: string, skuCode: string | null) => {
    if (skuCode) {
      return (
        <a href={`https://gamersbase.store/game/${skuCode}`} target="_blank" rel="noopener noreferrer" className="text-decoration-none fw-medium">
          {name}
        </a>
      )
    }
    return <span className="fw-bold">{name}</span>
  }

  return (
    <div className="py-2 text-start">
      <div className="mb-4 d-flex justify-content-between align-items-center">
        <Link to={mergeId ? `/merges/${mergeId}` : '/'} className="btn btn-link p-0 text-decoration-none text-muted">
          &larr; Назад
        </Link>
        <h2 className="mb-0 fw-bold">Пользователь: {email}</h2>
        <Badge bg="light" className="text-dark border fw-normal">ID объединения: {mergeId || 'Последнее'}</Badge>
      </div>

      <Row className="mt-4 g-4">
        <Col lg={12}>
          <Card className="mb-4 border-0 shadow-sm">
            <Card.Header className="bg-white border-0 pt-4 px-4 fw-bold d-flex gap-03 align-items-center">
              <FaShoppingBag className="me-2 text-primary" />
              <span>Купленные товары</span>
            </Card.Header>
            <Card.Body className="px-4">
              {interactions.purchases.length > 0 ? (
                <ul className="list-unstyled mb-0">
                  {interactions.purchases.map((item: any) => (
                    <li key={item.id} className="mb-2 pb-2">
                      <span className="me-3 text-muted">{moment(item.datetime).format('YYYY-MM-DD HH:mm:ss')}</span>
                      {renderProductName(item.product_name, item.skuCode)}
                    </li>
                  ))}
                </ul>
              ) : (
                <div className="text-muted italic">Покупки не найдены</div>
              )}
            </Card.Body>
          </Card>
        </Col>

        <Col lg={6}>
          <Card className="border-0 shadow-sm">
            <Card.Header className="bg-white border-0 pt-4 px-4 fw-bold d-flex gap-03 align-items-center">
              <FaHistory className="me-2 text-secondary" />
              <span>Последние просмотры</span>
            </Card.Header>
            <Card.Body className="px-4">
            {interactions.views.length > 0 ? (
            <ul className="list-unstyled mb-0">
              {interactions.views.map((item: any) => (
                  <li key={item.id} className="mb-2 pb-2">
                    <span className="me-3 text-muted">{moment(item.datetime).format('YYYY-MM-DD HH:mm:ss')}</span>
                    {renderProductName(item.product_name, item.skuCode)}
                  </li>
              ))}
            </ul>) : (<span>Просмотры не найдены</span>)}
            </Card.Body>
          </Card>
        </Col>

        <Col lg={6}>
          <Card className="border-0 shadow-sm overflow-hidden mb-3">
            <Card.Header className="bg-white border-0 pt-4 px-4 fw-bold">
              <FaMagic className="me-2 text-info" /> Рекомендации
            </Card.Header>

            <Card.Body className="p-0">

              <Table hover className="mb-0 text-start">
                <thead className="bg-light border-bottom">
                  <tr className="align-middle">
                    <th className="ps-4 fw-bold small text-uppercase text-muted py-3">Товар</th>
                    <th className="fw-bold small text-uppercase text-muted py-3">Модель</th>
                    <th className="ps-3 fw-bold small text-uppercase text-muted py-3">Оценка</th>
                  </tr>
                </thead>
                <tbody>
                  {recommendations.length > 0 ? (
                    recommendations.map((rec: any, index: number) => (
                      <tr key={`${rec.product_id}-${index}`} className="align-middle">
                        <td className="ps-4 py-3">{renderProductName(rec.product_name, rec.skuCode)}</td>
                        <td className="py-3"><Badge bg="light" className="text-muted border rounded-pill px-3 fw-normal">{rec.model_name}</Badge></td>
                        <td className="ps-3 py-3 font-monospace">{rec.score.toFixed(4)}</td>
                      </tr>
                    ))
                  ) : (
                    <tr>
                      <td colSpan={3} className="py-5 text-muted italic">Для этого пользователя рекомендации не найдены</td>
                    </tr>
                  )}
                </tbody>
              </Table>
            </Card.Body>
            <Card.Footer className="bg-light border-0 text-muted small px-4 py-3">
              Рекомендации ранжированы на основе нескольких моделей (лимит 10 позиций).
            </Card.Footer>
          </Card>

          {excludedRecommendations.length > 0 && (
          <Card className="border-0 shadow-sm overflow-hidden">
            <Card.Header className="bg-white border-0 pt-4 px-4 fw-bold">
              <FaMagic className="me-2 text-warning" /> Исключенные рекомендации
            </Card.Header>
            <Card.Body  className="p-0">
                    <Table hover className="mb-0 text-start bg-body-secondary">
                      <thead className="bg-light border-bottom">
                      <tr className="align-middle">
                        <th className="ps-4 fw-bold small text-uppercase text-muted py-3">Товар</th>
                        <th className="fw-bold small text-uppercase text-muted py-3">Модель</th>
                        <th className="ps-3 fw-bold small text-uppercase text-muted py-3">Оценка</th>
                      </tr>
                      </thead>
                      <tbody>
                      {excludedRecommendations.map((ex: any, index: number) => (
                          <tr key={`ex-${ex.product_id}-${index}`} className='text-muted'>
                            <td className="ps-4 py-3">{renderProductName(ex.product_name, ex.skuCode)}</td>
                            <td className="py-3"><Badge bg="light" className="text-muted border rounded-pill px-3 fw-normal">{ex.model_name}</Badge></td>
                            <td className="ps-3 py-3 font-monospace">{ex.score ? ex.score.toFixed(4) : 0.00}</td>
                          </tr>
                      ))}
                      </tbody>
                    </Table>
            </Card.Body>
          </Card>
          )}
        </Col>
      </Row>
    </div>
  )
}

export default UserPage
