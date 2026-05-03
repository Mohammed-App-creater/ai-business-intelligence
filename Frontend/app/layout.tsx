import type { Metadata } from "next";
import "./globals.css";

export const metadata: Metadata = {
  title: "LEO AI BI — Internal Test UI",
  description: "Internal testing console for the LEO AI BI Assistant",
};

export default function RootLayout({
  children,
}: {
  children: React.ReactNode;
}) {
  return (
    <html lang="en">
      <body className="font-sans">{children}</body>
    </html>
  );
}
