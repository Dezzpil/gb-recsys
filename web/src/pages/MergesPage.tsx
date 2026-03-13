import { useQuery } from '@tanstack/react-query'
import { getMerges } from '../api/api'
import { Table, Spinner, Alert } from 'react-bootstrap'
import { Link } from 'react-router-dom'
import { FaInfoCircle } from 'react-icons/fa'
import moment from 'moment'

const MergesPage = () => {
  const { data: merges, isLoading, error } = useQuery({
    queryKey: ['merges'],
    queryFn: getMerges
  })

  if (isLoading) return <div className="text-center mt-5"><Spinner animation="border" /></div>
  if (error) return <Alert variant="danger">Ошибка при загрузке объединений</Alert>

  return (
    <div className="py-2 text-start">
      <h2 className="mb-4 fw-bold">Логи объединения данных</h2>
      <Table hover responsive className="bg-white text-start">
        <thead style={{ backgroundColor: '#f8f9fa', borderBottom: '2px solid #dee2e6' }}>
          <tr className="align-middle">
            <th className="fw-bold">ID</th>
            <th className="fw-bold">Время начала</th>
            <th className="fw-bold">Emails</th>
            <th className="fw-bold">Товары</th>
            <th className="fw-bold">Взаимодействия</th>
            <th className="fw-bold">Действия</th>
          </tr>
        </thead>
        <tbody>
          {merges.map((merge: any) => (
            <tr key={merge.id} className="align-middle">
              <td className="py-3">{merge.id}</td>
              <td className="py-3">{moment(merge.start_time).format('YYYY-MM-DD HH:mm:ss')}</td>
              <td className="py-3">{merge.unique_emails_count}</td>
              <td className="py-3">{merge.unique_products_count}</td>
              <td className="py-3">{merge.interactions_count !== null ? merge.interactions_count : '-'}</td>
              <td className="py-3">
                <Link to={`/merges/${merge.id}`} className="btn btn-outline-primary btn-sm rounded-pill px-3">
                  <FaInfoCircle className="me-1" /> Детали
                </Link>
              </td>
            </tr>
          ))}
        </tbody>
      </Table>
    </div>
  )
}

export default MergesPage
