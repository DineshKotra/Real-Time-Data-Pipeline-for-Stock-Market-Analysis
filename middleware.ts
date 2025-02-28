import { NextResponse } from "next/server"
import type { NextRequest } from "next/server"
import { AuthService } from "@/lib/auth/auth-service"

// Add paths that don't require authentication
const publicPaths = ["/auth/login", "/auth/register", "/auth/forgot-password"]

export async function middleware(request: NextRequest) {
  const session = await AuthService.verifySession()
  const { pathname } = request.nextUrl

  // Allow public paths
  if (publicPaths.includes(pathname)) {
    if (session) {
      // If user is already logged in, redirect to dashboard
      return NextResponse.redirect(new URL("/", request.url))
    }
    return NextResponse.next()
  }

  // Check authentication for protected routes
  if (!session) {
    const loginUrl = new URL("/auth/login", request.url)
    loginUrl.searchParams.set("from", pathname)
    return NextResponse.redirect(loginUrl)
  }

  return NextResponse.next()
}

export const config = {
  matcher: [
    /*
     * Match all request paths except:
     * - _next/static (static files)
     * - _next/image (image optimization files)
     * - favicon.ico (favicon file)
     * - public folder
     */
    "/((?!_next/static|_next/image|favicon.ico|public).*)",
  ],
}

