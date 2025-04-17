import 'bootstrap/dist/css/bootstrap.min.css';
import React, { useState } from "react";
import axios from "axios";
import {
    Container,
    Row,
    Col,
    Form,
    Button,
    Alert,
    Card,
    Table,
    Spinner,
    Pagination
} from "react-bootstrap";

function Interface() {
    const [query, setQuery] = useState("");
    const [result, setResult] = useState(null);
    const [error, setError] = useState(null);
    const [runtime, setRuntime] = useState(null);
    const [loading, setLoading] = useState(false);
    const [currentPage, setCurrentPage] = useState(1);
    const [pageInput, setPageInput] = useState(1);
    const rowsPerPage = 50;

    const runQuery = async () => {
        setError(null);
        setResult(null);
        setRuntime(null);
        setLoading(true);
        setCurrentPage(1);
        setPageInput(1);

        try {
            const response = await axios.post("http://localhost:8000/query", {
                query: query,
            });
            if (response.data.error) {
                setError(response.data.error);
            } else {
                setResult(response.data.result);
                setRuntime(response.data.runtime);
            }
        } catch (err) {
            setError("Request failed: " + err.message);
        } finally {
            setLoading(false);
        }
    };

    const indexOfLastRow = currentPage * rowsPerPage;
    const indexOfFirstRow = indexOfLastRow - rowsPerPage;
    const currentRows = result ? result.slice(indexOfFirstRow, indexOfLastRow) : [];

    const handlePageChange = (pageNumber) => {
        setCurrentPage(pageNumber);
        setPageInput(pageNumber);
    };

    const handlePageInputChange = (e) => {
        const pageNumber = parseInt(e.target.value, 10);
        if (pageNumber >= 1 && pageNumber <= Math.ceil(result.length / rowsPerPage)) {
            setPageInput(pageNumber);
            setCurrentPage(pageNumber);
        }
    };

    const handlePageInputSubmit = () => {
        const pageNumber = parseInt(pageInput, 10);
        if (pageNumber >= 1 && pageNumber <= Math.ceil(result.length / rowsPerPage)) {
            setCurrentPage(pageNumber);
        }
    };

    const pageCount = result ? Math.ceil(result.length / rowsPerPage) : 0;
    const maxPagesToShow = 10;
    let startPage = Math.max(1, currentPage - Math.floor(maxPagesToShow / 2));
    let endPage = Math.min(pageCount, startPage + maxPagesToShow - 1);

    if (endPage - startPage < maxPagesToShow - 1) {
        startPage = Math.max(1, endPage - maxPagesToShow + 1);
    }

    const paginationItems = [];
    for (let i = startPage; i <= endPage; i++) {
        paginationItems.push(
            <Pagination.Item key={i} active={i === currentPage} onClick={() => handlePageChange(i)}>
                {i}
            </Pagination.Item>
        );
    }


    return (
        <Container className="my-5">
            <Row className="justify-content-center">
                <Col md={8}>
                    <h2 className="mb-4 text-center">Project 3 Interface</h2>

                    <Form>
                        <Form.Group controlId="sqlQuery">
                            <Form.Label>Enter Query</Form.Label>
                            <Form.Control
                                as="textarea"
                                rows={6}
                                placeholder="Enter your SQL query here..."
                                value={query}
                                onChange={(e) => setQuery(e.target.value)}
                            />
                        </Form.Group>

                        <Button variant="primary" className="mt-3" onClick={runQuery}>
                            Run Query
                        </Button>
                    </Form>

                    {error && (
                        <Alert variant="danger" className="mt-4">
                            {error}
                        </Alert>
                    )}

                    {loading && (
                        <div className="text-center my-4">
                            <Spinner animation="border" variant="primary" role="status">
                                <span className="visually-hidden">Loading...</span>
                            </Spinner>
                        </div>
                    )}

                    {result && result.length > 0 && (
                        <Card className="mt-4">
                            {runtime ? (
                                <Card.Header>Query Result (executed in {runtime} s)</Card.Header>
                            ) : (
                                <Card.Header>Query Result</Card.Header>
                            )}
                            <Card.Body>
                                <Table bordered striped hover responsive>
                                    <thead>
                                        <tr>
                                            {Object.keys(result[0]).map((key) => (
                                                <th key={key}>{key}</th>
                                            ))}
                                        </tr>
                                    </thead>
                                    <tbody>
                                        {currentRows.map((row, idx) => (
                                            <tr key={idx}>
                                                {Object.values(row).map((value, i) => (
                                                    <td key={i}>{value}</td>
                                                ))}
                                            </tr>
                                        ))}
                                    </tbody>
                                </Table>

                                {/* Pagination controls */}
                                <Pagination className="mt-4">
                                    {/* Jump to first page */}
                                    {currentPage > 1 && (
                                        <Pagination.First onClick={() => handlePageChange(1)} />
                                    )}

                                    {/* Previous button */}
                                    {currentPage > 1 && (
                                        <Pagination.Prev onClick={() => handlePageChange(currentPage - 1)} />
                                    )}

                                    {/* Page numbers */}
                                    {paginationItems}

                                    {/* Next button */}
                                    {currentPage < pageCount && (
                                        <Pagination.Next onClick={() => handlePageChange(currentPage + 1)} />
                                    )}

                                    {/* Jump to last page */}
                                    {currentPage < pageCount && (
                                        <Pagination.Last onClick={() => handlePageChange(pageCount)} />
                                    )}
                                </Pagination>

                                {/* Page input box */}
                                <div className="mt-3">
                                    <Form.Group controlId="pageInput">
                                        <Form.Label>Go to Page</Form.Label>
                                        <div className="d-flex">
                                            <Form.Control
                                                type="number"
                                                min="1"
                                                max={pageCount}
                                                value={pageInput}
                                                onChange={handlePageInputChange}
                                            />
                                            <Button variant="primary" className="ml-2" onClick={handlePageInputSubmit}>
                                                Go
                                            </Button>
                                        </div>
                                    </Form.Group>
                                </div>
                            </Card.Body>
                        </Card>
                    )}
                </Col>
            </Row>
        </Container>
    );
}

export default Interface;
