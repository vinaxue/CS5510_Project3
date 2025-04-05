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
} from "react-bootstrap";

function Interface() {
    const [query, setQuery] = useState("");
    const [result, setResult] = useState(null);
    const [error, setError] = useState(null);

    const runQuery = async () => {
        setError(null);
        setResult(null);

        try {
            const response = await axios.post("http://localhost:8000/query", {
                query: query,
            });
            if (response.data.error) {
                setError(response.data.error);
            } else {
                setResult(response.data.result);
            }
        } catch (err) {
            setError("Request failed: " + err.message);
        }
    };

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

                    {/* TODO: fix the result once determine what the data looks like */}
                    {result && (
                        <Card className="mt-4">
                            <Card.Header>Query Result</Card.Header>
                            <Card.Body>
                                <pre>{JSON.stringify(result, null, 2)}</pre>
                            </Card.Body>
                        </Card>
                    )}
                </Col>
            </Row>
        </Container>
    );
}

export default Interface;
