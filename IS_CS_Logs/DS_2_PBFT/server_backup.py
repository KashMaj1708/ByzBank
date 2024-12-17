import grpc
from concurrent import futures
import time
import pbft_pb2
import pbft_pb2_grpc
from cryptography.hazmat.primitives import hashes
from cryptography.hazmat.primitives.asymmetric import padding
from cryptography.hazmat.primitives import serialization
from datetime import datetime

def load_public_key():
    with open("public_key.pem", "rb") as f:
        return serialization.load_pem_public_key(f.read())

class PBFTServiceServicer(pbft_pb2_grpc.PBFTServiceServicer):
    def PBFTMethod(self, request, context):
        try:
            public_key = load_public_key()
            
            # Verify the signature
            try:
                public_key.verify(
                    request.signature,
                    request.name.encode(),
                    padding.PSS(
                        mgf=padding.MGF1(hashes.SHA256()),
                        salt_length=padding.PSS.MAX_LENGTH
                    ),
                    hashes.SHA256()
                )
                print("Key verified")
            except Exception as e:
                context.set_code(grpc.StatusCode.INVALID_ARGUMENT)
                context.set_details(f"Signature verification failed: {str(e)}")
                raise
            
            # Log incoming request
            print(f"Received request - Name: {request.name}")
            print(f"Value: {request.value}")
            print(f"Message: {request.message}")  # Print the new message field
            timestamp = datetime.fromtimestamp(request.timestamp)  # Convert Unix timestamp to datetime
            print(f"Timestamp: {timestamp}")
            
            # Process the request (example business logic)
            result = f"PBFT Service Processing Complete"
            
            # Create and return response
            return pbft_pb2.PBFTResponse(result=result)
            
        except Exception as e:
            # Handle errors
            context.set_code(grpc.StatusCode.INTERNAL)
            context.set_details(f'Internal error: {str(e)}')
            raise

def serve():
    # Create a gRPC server
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    
    # Add the servicer to the server
    pbft_pb2_grpc.add_PBFTServiceServicer_to_server(
        PBFTServiceServicer(), server
    )
    
    # Select port
    port = 50051
    server.add_insecure_port(f'[::]:{port}')
    
    # Start the server
    server.start()
    print(f"Server started on port {port}")
    
    try:
        # Keep the server running
        while True:
            time.sleep(86400)  # One day in seconds
    except KeyboardInterrupt:
        # Handle graceful shutdown
        print("Shutting down server...")
        server.stop(grace=None)  # Immediately stop server
        print("Server stopped.")

if __name__ == '__main__':
    serve()