import { useState, useEffect } from 'react'
import { useParams, Link } from 'react-router-dom'
import { useQuery } from '@tanstack/react-query'
import { getMergeDetails, getMergeUsers } from '../api/api'
import { Table, Row, Col, Card, Spinner, Alert, Badge, Form, Pagination } from 'react-bootstrap'
import { FaUser, FaSortAmountDown, FaSortAmountUp, FaSearch } from 'react-icons/fa'
import { useForm } from 'react-hook-form'
import moment from 'moment'

const MergeDetailsPage = () => {
  const { id } = useParams<{ id: string }>()
  const mergeId = Number(id)
  
  const [sort, setSort] = useState({ by: 'interactions', order: 'desc' })
  const [page, setPage] = useState(1)
  const limit = 20
  
  const { register, watch } = useForm({ defaultValues: { search: '' } })
  const search = watch('search')

  useEffect(() => {
    setPage(1)
  }, [search])

  const detailsQuery = useQuery({
    queryKey: ['merge', mergeId],
    queryFn: () => getMergeDetails(mergeId)
  })

  const usersQuery = useQuery({
    queryKey: ['merge-users', mergeId, sort, page, search],
    queryFn: () => getMergeUsers(mergeId, { 
      sort_by: sort.by, 
      order: sort.order,
      limit,
      offset: (page - 1) * limit,
      search: search || undefined
    }),
    placeholderData: (previousData) => previousData
  })

  const toggleSort = (by: string) => {
    setSort(prev => ({
      by,
      order: prev.by === by && prev.order === 'desc' ? 'asc' : 'desc'
    }))
    setPage(1)
  }

  const isLoading = detailsQuery.isLoading || usersQuery.isLoading
  const error = detailsQuery.error || usersQuery.error

  if (isLoading) return <div className="text-center mt-5"><Spinner animation="border" /></div>
  if (error) return <Alert variant="danger">Ошибка при загрузке деталей объединения</Alert>

  const merge = detailsQuery.data
  const usersData = usersQuery.data || { users: [], total: 0 }
  const users = usersData.users
  const totalPages = Math.ceil(usersData.total / limit)

  return (
    <div className="py-2">
      <h2 className="mb-4 fw-bold">Объединение #{merge.id}</h2>
      
      <Row className="mb-5 g-4 text-start">
        <Col xs={12}>
          <Card className="border-0 shadow-sm">
            <Card.Header className="bg-transparent border-0 fw-bold pt-4 px-4">Основная информация и статистика</Card.Header>
            <Card.Body className="p-4">
              <Row>
                <Col md={6}>
                  <div className="mb-2">
                    <div className="text-muted small">Время начала</div>
                    <div className="fw-medium">{moment(merge.start_time).format('YYYY-MM-DD HH:mm:ss')}</div>
                  </div>
                  <div className="mb-2">
                    <div className="text-muted small">Длительность</div>
                    <div className="fw-medium">{merge.duration.toFixed(2)} сек.</div>
                  </div>
                </Col>
                <Col md={6}>
                  <div className="mb-2">
                    <div className="text-muted small">Файл Метрики</div>
                    <div className="fw-medium text-break">{merge.metrika_file}</div>
                  </div>
                  <div>
                    <div className="text-muted small">Файл заказов</div>
                    <div className="fw-medium text-break">{merge.orders_file}</div>
                  </div>
                </Col>
              </Row>
              <hr className="my-4 opacity-10" />
              <Row className="mb-4">
                <Col xs={4}>
                  <div className="text-muted small">Пользователи</div>
                  <div className="fs-4 fw-bold text-primary">{merge.unique_emails_count}</div>
                </Col>
                <Col xs={4}>
                  <div className="text-muted small">Товары</div>
                  <div className="fs-4 fw-bold text-success">{merge.unique_products_count}</div>
                </Col>
                <Col xs={4}>
                  <div className="text-muted small">Всего рекомендаций</div>
                  <div className="fs-4 fw-bold text-info">{merge.total_recommendations}</div>
                </Col>
              </Row>
              <hr className="my-4 opacity-10" />
              <h6 className="fw-bold mb-3 small text-uppercase text-muted">Рекомендации по моделям</h6>
              <div className="d-flex flex-wrap gap-4">
                {merge.recommendations_stats.map((stat: any) => (
                  <div key={stat.model_name} className="d-flex align-items-center mb-2">
                    <span className="small font-monospace text-secondary me-2">{stat.model_name}:</span>
                    <Badge bg="light" className="text-dark border font-monospace fw-normal">{stat.count}</Badge>
                  </div>
                ))}
              </div>
            </Card.Body>
          </Card>
        </Col>
      </Row>

      <Row className="mb-5">
        <Col xs={12}>
          <Card className="border-0 shadow-sm overflow-hidden">
            <Card.Header className="bg-transparent border-0 fw-bold pt-4 px-4">Распределение популярности продуктов</Card.Header>
            <Card.Body className="p-0">
              <div className="bg-light d-flex justify-content-center align-items-center" style={{ minHeight: '200px' }}>
                <img 
                  src={merge.plot_url} 
                  alt="Distribution Plot" 
                  className="img-fluid w-100"
                  style={{ maxHeight: '500px', objectFit: 'contain' }}
                  onError={(e) => {
                    const target = e.target as HTMLImageElement;
                    const card = target.closest('.card') as HTMLElement;
                    if (card) card.style.display = 'none';
                  }}
                />
              </div>
            </Card.Body>
          </Card>
        </Col>
      </Row>

      <div className="d-flex justify-content-between align-items-end mb-4">
        <div>
          <h4 className="mb-1 fw-bold">Пользователи</h4>
          <p className="text-muted small mb-0">Список всех пользователей в этом объединении</p>
        </div>
        <div style={{ width: '400px' }}>
          <Form.Group className="position-relative">
            <Form.Control 
              className="ps-5 bg-light border-0 rounded-pill py-2"
              placeholder="Поиск по email..." 
              {...register('search')}
            />
            <FaSearch className="position-absolute top-50 start-0 translate-middle-y ms-3 text-muted" style={{ fontSize: '0.9rem' }} />
          </Form.Group>
        </div>
      </div>

      <Table hover responsive className="bg-white text-start">
        <thead style={{ backgroundColor: '#f8f9fa', borderBottom: '2px solid #dee2e6' }}>
          <tr className="align-middle">
            <th onClick={() => toggleSort('email')} style={{ cursor: 'pointer' }} className="fw-bold">
              Email {sort.by === 'email' && (sort.order === 'desc' ? <FaSortAmountDown className="ms-1" /> : <FaSortAmountUp className="ms-1" />)}
            </th>
            <th onClick={() => toggleSort('purchases')} style={{ cursor: 'pointer' }} className="fw-bold">
              Куплено {sort.by === 'purchases' && (sort.order === 'desc' ? <FaSortAmountDown className="ms-1" /> : <FaSortAmountUp className="ms-1" />)}
            </th>
            <th onClick={() => toggleSort('interactions')} style={{ cursor: 'pointer' }} className="fw-bold">
              Взаимодействия {sort.by === 'interactions' && (sort.order === 'desc' ? <FaSortAmountDown className="ms-1" /> : <FaSortAmountUp className="ms-1" />)}
            </th>
            <th className="fw-bold">Действия</th>
          </tr>
        </thead>
        <tbody>
          {users.map((user: any) => (
            <tr key={user.email} className="align-middle">
              <td className="py-3">{user.email}</td>
              <td className="py-3">
                <Badge bg={user.purchases_count > 0 ? 'success' : 'light'} className={user.purchases_count > 0 ? '' : 'text-muted border'}>
                  {user.purchases_count}
                </Badge>
              </td>
              <td className="py-3">{user.interactions_count}</td>
              <td className="py-3">
                <Link to={`/users/${user.email}?merge_id=${mergeId}`} className="btn btn-outline-primary btn-sm rounded-pill px-3">
                  <FaUser className="me-1" /> Профиль
                </Link>
              </td>
            </tr>
          ))}
        </tbody>
      </Table>

      {totalPages > 1 && (
        <div className="d-flex justify-content-center mt-4">
          <Pagination>
            <Pagination.First onClick={() => setPage(1)} disabled={page === 1} />
            <Pagination.Prev onClick={() => setPage(p => Math.max(1, p - 1))} disabled={page === 1} />
            
            {[...Array(Math.min(5, totalPages))].map((_, i) => {
              let pageNum;
              if (totalPages <= 5) {
                pageNum = i + 1;
              } else if (page <= 3) {
                pageNum = i + 1;
              } else if (page >= totalPages - 2) {
                pageNum = totalPages - 4 + i;
              } else {
                pageNum = page - 2 + i;
              }
              return (
                <Pagination.Item 
                  key={pageNum} 
                  active={pageNum === page}
                  onClick={() => setPage(pageNum)}
                >
                  {pageNum}
                </Pagination.Item>
              );
            })}

            <Pagination.Next onClick={() => setPage(p => Math.min(totalPages, p + 1))} disabled={page === totalPages} />
            <Pagination.Last onClick={() => setPage(totalPages)} disabled={page === totalPages} />
          </Pagination>
        </div>
      )}
    </div>
  )
}

export default MergeDetailsPage
