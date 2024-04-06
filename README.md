# BMAPS API

BMAPS API built with Python and FastAPI

---

## 📄 Get started

BMAPS API is used to provide end-to-end backend services for the BMAPS application. </br>
The postman collection is meintained [here].

---

## **Overview**

The collection provides API endpoint information for all 12 BMAPS services other than the `authentication`. The services covered are listed below:

1. Strategic plan `coming soon`
2. Range Architecture `coming soon`
3. Open to Buy `coming soon`
4. Assortment Plan `coming soon`
5. Budget Plan `coming soon`
6. WSSI_MSSI `coming soon`
7. Merchandise Financial Plan `coming soon`
8. Sales Forecasting `coming soon`
9. Margin Planning `coming soon`
10. Option Plan `coming soon`
11. Store Grading `coming soon`
12. Auto Stock Replenishment `coming soon`

## **Getting started guide**

To start using the BMAPS APIs, you need to -

- You must have the required access and rights to access the BMAPS project services
- The API uses an `authentication token` and a `refresh token` to validate a user trying to access the services, you may get the tokens using the `/auth/login` endpoint.
- The API only responds to HTTPS-secured communications in production. Any requests sent via HTTP return an HTTP 301 redirect to the corresponding HTTPS resources.
- The API returns request responses in JSON format. When an API request returns an error, it is sent in the JSON response with error message.

## Authentication

The BMAPS API uses `access token` for authentication.

You must include an `access token` in each request to the Postman API with the `X-API-Token` request header.

### Authentication error response

If an `access token` is missing, malformed, or invalid, you will receive an HTTP 401 Unauthorised response code.

## Rate and usage limits

As of now, We haven't defined any rate limit in the API. However, the we may apply some strict usage policies in future.

## **Need some help?**

Contact the author of this documentation.
