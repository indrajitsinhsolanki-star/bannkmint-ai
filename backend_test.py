#!/usr/bin/env python3
"""
BannkMint AI Backend API Testing Suite
Tests all backend endpoints with comprehensive scenarios
"""

import requests
import sys
import json
from datetime import datetime, timedelta
import os

class BannkMintAPITester:
    def __init__(self, base_url="https://bankdata-sync-1.preview.emergentagent.com"):
        self.base_url = base_url
        self.api_url = f"{base_url}/api"
        self.tests_run = 0
        self.tests_passed = 0
        self.api_key = "dev-key"

    def log_test(self, test_name, success, details=""):
        """Log test results"""
        self.tests_run += 1
        if success:
            self.tests_passed += 1
            print(f"✅ {test_name}: PASSED {details}")
        else:
            print(f"❌ {test_name}: FAILED {details}")
        return success

    def test_health_endpoint(self):
        """Test GET /api/health endpoint"""
        try:
            response = requests.get(f"{self.api_url}/health", timeout=10)
            expected_response = {"status": "ok"}
            
            success = (response.status_code == 200 and 
                      response.json() == expected_response)
            
            details = f"(Status: {response.status_code}, Response: {response.json()})"
            return self.log_test("Health Check", success, details)
            
        except Exception as e:
            return self.log_test("Health Check", False, f"(Error: {str(e)})")

    def test_csv_upload_valid(self):
        """Test POST /api/uploads/transactions-csv with valid CSV"""
        try:
            # Read the demo CSV file
            with open('/app/demo.csv', 'rb') as f:
                files = {'file': ('demo.csv', f, 'text/csv')}
                headers = {'x-api-key': self.api_key}
                
                response = requests.post(
                    f"{self.api_url}/uploads/transactions-csv",
                    files=files,
                    headers=headers,
                    timeout=30
                )
            
            success = response.status_code == 200
            if success:
                data = response.json()
                # First upload should import 8 transactions
                expected_imported = 8
                expected_skipped = 0
                success = (data.get('imported') == expected_imported and 
                          data.get('skipped') == expected_skipped)
                details = f"(Status: {response.status_code}, Imported: {data.get('imported')}, Skipped: {data.get('skipped')})"
            else:
                details = f"(Status: {response.status_code}, Response: {response.text})"
                
            return self.log_test("CSV Upload - Valid File", success, details)
            
        except Exception as e:
            return self.log_test("CSV Upload - Valid File", False, f"(Error: {str(e)})")

    def test_csv_upload_duplicate(self):
        """Test CSV upload deduplication (re-upload same file)"""
        try:
            # Upload the same file again to test deduplication
            with open('/app/demo.csv', 'rb') as f:
                files = {'file': ('demo.csv', f, 'text/csv')}
                headers = {'x-api-key': self.api_key}
                
                response = requests.post(
                    f"{self.api_url}/uploads/transactions-csv",
                    files=files,
                    headers=headers,
                    timeout=30
                )
            
            success = response.status_code == 200
            if success:
                data = response.json()
                # Second upload should skip all 8 transactions
                expected_imported = 0
                expected_skipped = 8
                success = (data.get('imported') == expected_imported and 
                          data.get('skipped') == expected_skipped)
                details = f"(Status: {response.status_code}, Imported: {data.get('imported')}, Skipped: {data.get('skipped')})"
            else:
                details = f"(Status: {response.status_code}, Response: {response.text})"
                
            return self.log_test("CSV Upload - Deduplication", success, details)
            
        except Exception as e:
            return self.log_test("CSV Upload - Deduplication", False, f"(Error: {str(e)})")

    def test_csv_upload_no_api_key(self):
        """Test CSV upload without API key (should fail)"""
        try:
            with open('/app/demo.csv', 'rb') as f:
                files = {'file': ('demo.csv', f, 'text/csv')}
                # No x-api-key header
                
                response = requests.post(
                    f"{self.api_url}/uploads/transactions-csv",
                    files=files,
                    timeout=30
                )
            
            # Should return 401 Unauthorized
            success = response.status_code == 401
            details = f"(Status: {response.status_code})"
            return self.log_test("CSV Upload - No API Key", success, details)
            
        except Exception as e:
            return self.log_test("CSV Upload - No API Key", False, f"(Error: {str(e)})")

    def test_csv_upload_invalid_api_key(self):
        """Test CSV upload with invalid API key (should fail)"""
        try:
            with open('/app/demo.csv', 'rb') as f:
                files = {'file': ('demo.csv', f, 'text/csv')}
                headers = {'x-api-key': 'invalid-key'}
                
                response = requests.post(
                    f"{self.api_url}/uploads/transactions-csv",
                    files=files,
                    headers=headers,
                    timeout=30
                )
            
            # Should return 401 Unauthorized
            success = response.status_code == 401
            details = f"(Status: {response.status_code})"
            return self.log_test("CSV Upload - Invalid API Key", success, details)
            
        except Exception as e:
            return self.log_test("CSV Upload - Invalid API Key", False, f"(Error: {str(e)})")

    def test_csv_upload_non_csv_file(self):
        """Test upload with non-CSV file (should fail)"""
        try:
            # Create a temporary text file
            test_content = "This is not a CSV file"
            files = {'file': ('test.txt', test_content, 'text/plain')}
            headers = {'x-api-key': self.api_key}
            
            response = requests.post(
                f"{self.api_url}/uploads/transactions-csv",
                files=files,
                headers=headers,
                timeout=30
            )
        
            # Should return 415 Unsupported Media Type
            success = response.status_code == 415
            details = f"(Status: {response.status_code})"
            return self.log_test("CSV Upload - Non-CSV File", success, details)
            
        except Exception as e:
            return self.log_test("CSV Upload - Non-CSV File", False, f"(Error: {str(e)})")

    def test_get_transactions_default(self):
        """Test GET /api/transactions without filters (last 30 days)"""
        try:
            response = requests.get(f"{self.api_url}/transactions", timeout=10)
            
            success = response.status_code == 200
            if success:
                data = response.json()
                required_fields = ['data', 'page', 'limit', 'total']
                success = all(field in data for field in required_fields)
                
                if success:
                    # Should have transactions from our upload
                    success = data['total'] >= 8  # At least 8 transactions
                    details = f"(Status: {response.status_code}, Total: {data['total']}, Page: {data['page']}, Limit: {data['limit']})"
                else:
                    details = f"(Status: {response.status_code}, Missing fields in response)"
            else:
                details = f"(Status: {response.status_code}, Response: {response.text})"
                
            return self.log_test("Get Transactions - Default", success, details)
            
        except Exception as e:
            return self.log_test("Get Transactions - Default", False, f"(Error: {str(e)})")

    def test_get_transactions_with_date_filter(self):
        """Test GET /api/transactions with date filters"""
        try:
            # Filter for January 2024 (should include our demo data)
            params = {
                'from_date': '2024-01-01',
                'to_date': '2024-01-31'
            }
            
            response = requests.get(f"{self.api_url}/transactions", params=params, timeout=10)
            
            success = response.status_code == 200
            if success:
                data = response.json()
                # Should have our 8 transactions from January 2024
                success = data['total'] >= 8
                details = f"(Status: {response.status_code}, Total: {data['total']} transactions in Jan 2024)"
            else:
                details = f"(Status: {response.status_code}, Response: {response.text})"
                
            return self.log_test("Get Transactions - Date Filter", success, details)
            
        except Exception as e:
            return self.log_test("Get Transactions - Date Filter", False, f"(Error: {str(e)})")

    def test_get_transactions_with_pagination(self):
        """Test GET /api/transactions with pagination"""
        try:
            params = {
                'page': 1,
                'limit': 5
            }
            
            response = requests.get(f"{self.api_url}/transactions", params=params, timeout=10)
            
            success = response.status_code == 200
            if success:
                data = response.json()
                success = (data['page'] == 1 and 
                          data['limit'] == 5 and
                          len(data['data']) <= 5)
                details = f"(Status: {response.status_code}, Page: {data['page']}, Limit: {data['limit']}, Returned: {len(data['data'])})"
            else:
                details = f"(Status: {response.status_code}, Response: {response.text})"
                
            return self.log_test("Get Transactions - Pagination", success, details)
            
        except Exception as e:
            return self.log_test("Get Transactions - Pagination", False, f"(Error: {str(e)})")

    def run_all_tests(self):
        """Run all backend API tests"""
        print("🚀 Starting BannkMint AI Backend API Tests")
        print(f"📍 Testing API at: {self.api_url}")
        print("=" * 60)
        
        # Test health endpoint first
        if not self.test_health_endpoint():
            print("\n❌ Health check failed - API may be down. Stopping tests.")
            return False
        
        # Test CSV upload functionality
        self.test_csv_upload_valid()
        self.test_csv_upload_duplicate()
        self.test_csv_upload_no_api_key()
        self.test_csv_upload_invalid_api_key()
        self.test_csv_upload_non_csv_file()
        
        # Test transaction retrieval
        self.test_get_transactions_default()
        self.test_get_transactions_with_date_filter()
        self.test_get_transactions_with_pagination()
        
        # Print summary
        print("\n" + "=" * 60)
        print(f"📊 Test Summary: {self.tests_passed}/{self.tests_run} tests passed")
        
        if self.tests_passed == self.tests_run:
            print("🎉 All tests passed! Backend API is working correctly.")
            return True
        else:
            failed_tests = self.tests_run - self.tests_passed
            print(f"⚠️  {failed_tests} test(s) failed. Please check the issues above.")
            return False

def main():
    """Main test execution"""
    tester = BannkMintAPITester()
    success = tester.run_all_tests()
    return 0 if success else 1

if __name__ == "__main__":
    sys.exit(main())