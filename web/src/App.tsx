import { Routes, Route, Link } from 'react-router-dom'
import { Container, Navbar, Nav } from 'react-bootstrap'
import MergesPage from './pages/MergesPage'
import MergeDetailsPage from './pages/MergeDetailsPage'
import UserPage from './pages/UserPage'

function App() {
  return (
    <>
      <Navbar style={{ backgroundColor: '#2e303a' }} variant="dark" expand="lg" className="py-3">
        <Container fluid className="px-4">
          <Navbar.Brand as={Link} to="/" className="d-flex align-items-center">
            <img 
              src="https://gamersbase.store/src/images/themes/assets/images/site-logo.svg"
              alt="GamersBase Logo" 
              height="40"
              className="me-2"
            />
          </Navbar.Brand>
          <Navbar.Toggle aria-controls="basic-navbar-nav" />
          <Navbar.Collapse id="basic-navbar-nav" className="align-items-center">
            <Nav className="me-auto align-items-center">
              <Nav.Link as={Link} to="/">Объединения</Nav.Link>
            </Nav>
          </Navbar.Collapse>
        </Container>
      </Navbar>

      <Container fluid className="mt-4 pb-5 px-4">
        <Routes>
          <Route path="/" element={<MergesPage />} />
          <Route path="/merges/:id" element={<MergeDetailsPage />} />
          <Route path="/users/:email" element={<UserPage />} />
        </Routes>
      </Container>
    </>
  )
}

export default App
