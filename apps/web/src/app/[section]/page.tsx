import { Terminal } from "@/components/terminal";
export default async function Page({
  params,
}: {
  params: Promise<{ section: string }>;
}) {
  const { section } = await params;
  return <Terminal section={section} />;
}
